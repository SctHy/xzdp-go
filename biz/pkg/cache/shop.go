package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/coocood/freecache"
	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"

	"xzdp/biz/model/shop"
	"xzdp/conf"
)

type ShopSource struct {
	Detail func(context.Context, int64) (*shop.Shop, error)
	Types  func(context.Context) ([]*shop.ShopType, error)
	Exists func(context.Context, int64) (bool, error)
	IDs    func(context.Context) ([]int64, error)
}

type ShopCache struct {
	invalidations atomic.Uint64
	rebuild       chan struct{}

	cfg             conf.ShopCache
	prefix          string
	local           *freecache.Cache
	redis           *redis.Client
	source          ShopSource
	flight          singleflight.Group
	mu              sync.Mutex // Serializes invalidation with local fills.
	epoch           uint64
	filter          atomic.Pointer[bloom.BloomFilter]
	hits, misses    [2]atomic.Uint64
	loads           [2]atomic.Uint64
	existenceProbes atomic.Uint64
	rejected        atomic.Uint64
}

func NewShopCache(env string, cfg conf.ShopCache, rdb *redis.Client, source ShopSource) *ShopCache {
	return &ShopCache{rebuild: make(chan struct{}, 1), cfg: cfg, prefix: "xzdp:" + env + ":shop:", local: freecache.NewCache(cfg.MemoryMiB << 20), redis: rdb, source: source}
}

func (s *ShopCache) Detail(ctx context.Context, id int64) (*shop.Shop, error) {
	var result shop.Shop
	if err := s.detail(ctx, id, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DetailJSON lets HTTP reuse the serialized shop shared by both cache levels.
func (s *ShopCache) DetailJSON(ctx context.Context, id int64) (json.RawMessage, error) {
	var data json.RawMessage
	err := s.detail(ctx, id, &data)
	return data, err
}

func (s *ShopCache) detail(ctx context.Context, id int64, out any) error {
	if id <= 0 {
		return gorm.ErrRecordNotFound
	}
	suffix := strconv.FormatInt(id, 10)
	filter := s.filter.Load()
	absent := filter != nil && !filter.TestString(suffix)
	if absent && s.cfg.TrustBloom {
		s.rejected.Add(1)
		return gorm.ErrRecordNotFound
	}
	return s.read(ctx, "detail:"+suffix, out, func(ctx context.Context) (any, error) {
		if absent {
			s.existenceProbes.Add(1)
			exists, err := s.source.Exists(ctx, id)
			if err != nil {
				return nil, err
			}
			if !exists {
				s.rejected.Add(1)
				return nil, gorm.ErrRecordNotFound
			}
		}
		s.loads[0].Add(1)
		return s.source.Detail(ctx, id)
	})
}

func (s *ShopCache) Types(ctx context.Context) ([]*shop.ShopType, error) {
	var result []*shop.ShopType
	err := s.read(ctx, "types:all", &result, func(ctx context.Context) (any, error) {
		s.loads[1].Add(1)
		return s.source.Types(ctx)
	})
	return result, err
}

func decodeShop(data []byte, out any) error {
	if string(data) == "null" {
		return gorm.ErrRecordNotFound
	}
	if raw, ok := out.(*json.RawMessage); ok {
		*raw = data
		return nil
	}
	return json.Unmarshal(data, out)
}

func (s *ShopCache) read(ctx context.Context, suffix string, out any, load func(context.Context) (any, error)) error {
	key := s.prefix + suffix
	if data, err := s.local.Get([]byte(key)); err == nil {
		s.hits[0].Add(1)
		return decodeShop(data, out)
	}
	s.misses[0].Add(1)
	s.mu.Lock()
	generation := s.epoch
	s.mu.Unlock()
	// Include the generation so new readers never join a fill predating invalidation.
	ch := s.flight.DoChan(fmt.Sprintf("%d/%s", generation, key), func() (any, error) {
		if data, err := s.local.Get([]byte(key)); err == nil {
			return data, nil
		}
		// One caller disconnecting must not cancel a fill shared by other callers.
		work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		data, err := s.fill(work, key, load)
		if err == nil {
			ttl := s.cfg.LocalSeconds
			if string(data) == "null" {
				ttl = min(ttl, 3)
			}
			s.mu.Lock()
			if s.epoch == generation {
				_ = s.local.Set([]byte(key), data, ttl)
			}
			s.mu.Unlock()
		}
		return data, err
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return r.Err
		}
		return decodeShop(r.Val.([]byte), out)
	}
}

// The token fences Redis writes: an expired or invalidated lease cannot publish old data.
var finishFill = redis.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('PSETEX',KEYS[2],ARGV[2],ARGV[3]);redis.call('DEL',KEYS[1]);return 1 end return 0`)
var unlockFill = redis.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`)

// Reserve time for MySQL when Redis accepts a connection but does not reply.
func (s *ShopCache) lookup(ctx context.Context, key string) ([]byte, error) {
	probe, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return s.redis.Get(probe, key).Bytes()
}

func (s *ShopCache) fill(ctx context.Context, key string, load func(context.Context) (any, error)) ([]byte, error) {
	token := fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
	lock := key + ":fill"
	owns := false
	for {
		data, err := s.lookup(ctx, key)
		if err == nil {
			s.hits[1].Add(1)
			return data, nil
		}
		s.misses[1].Add(1)
		if !errors.Is(err, redis.Nil) {
			break
		}
		leaseCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		owns, err = s.redis.SetNX(leaseCtx, lock, token, 4*time.Second).Result()
		cancel()
		if err != nil {
			break
		}
		if owns {
			// A previous owner may have filled the cache between GET and SET NX.
			if data, err = s.lookup(ctx, key); err == nil {
				unlockFill.Run(ctx, s.redis, []string{lock}, token)
				return data, nil
			}
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}

	value, err := load(ctx)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		if owns {
			unlockFill.Run(ctx, s.redis, []string{lock}, token)
		}
		return nil, err
	}
	ttl := time.Duration(float64(time.Second) * float64(s.cfg.RedisSeconds) * (0.8 + 0.4*rand.Float64()))
	if errors.Is(err, gorm.ErrRecordNotFound) {
		value = nil
		ttl = 3 * time.Second
	}
	data, err := json.Marshal(value)
	if err == nil && owns {
		finishFill.Run(ctx, s.redis, []string{lock, key}, token, ttl.Milliseconds(), data)
	}
	return data, err
}

// Invalidate runs after the writer commits. id=0 selects the shop type list.
func (s *ShopCache) Invalidate(ctx context.Context, id int64) error {
	key := s.prefix + "types:all"
	if id != 0 {
		key = s.prefix + "detail:" + strconv.FormatInt(id, 10)
	}
	_, err := s.redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, key, key+":fill")
		p.Publish(ctx, s.prefix+"invalidate", key)
		return nil
	})
	s.evict(key)
	return err
}

func (s *ShopCache) evict(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	if key == "" {
		s.local.Clear()
	} else {
		s.local.Del([]byte(key))
	}
	// An event can represent a newly created shop.
	s.filter.Store(nil)
}

func (s *ShopCache) Subscribe(ctx context.Context) {
	sub := s.redis.Subscribe(ctx, s.prefix+"invalidate")
	defer sub.Close()
	events := sub.ChannelWithSubscriptions(ctx, 128)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			switch m := event.(type) {
			case *redis.Subscription:
				s.evict("")
				select {
				case s.rebuild <- struct{}{}:
				default:
				}
			case *redis.Message:
				s.evict(m.Payload)
				s.invalidations.Add(1)
			}
		}
	}
}

func (s *ShopCache) Refresh(ctx context.Context) error {
	s.mu.Lock()
	generation := s.epoch
	s.mu.Unlock()
	ids, err := s.source.IDs(ctx)
	if err != nil {
		return err
	}
	filter := bloom.NewWithEstimates(uint(max(1, len(ids))), 0.01)
	for _, id := range ids {
		filter.AddString(strconv.FormatInt(id, 10))
	}
	s.mu.Lock()
	if s.epoch == generation {
		s.filter.Store(filter)
	}
	s.mu.Unlock()
	if _, err = s.Types(ctx); err != nil {
		return err
	}
	for _, id := range s.cfg.HotIDs {
		if _, err = s.Detail(ctx, id); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return nil
}

func (s *ShopCache) Warm(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.RefreshSeconds) * time.Second)
	defer ticker.Stop()
	for {
		work, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.Refresh(work)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("shop cache refresh", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-s.rebuild:
		case <-ticker.C:
		}
	}
}

func (s *ShopCache) Metrics() string {
	var result string
	for i, level := range []string{"l1", "l2"} {
		hits, misses := s.hits[i].Load(), s.misses[i].Load()
		result += fmt.Sprintf("xzdp_shop_cache_hits_total{level=%q} %d\nxzdp_shop_cache_misses_total{level=%q} %d\nxzdp_shop_cache_hit_ratio{level=%q} %g\n", level, hits, level, misses, level, float64(hits)/float64(max(1, hits+misses)))
	}
	return result + fmt.Sprintf("xzdp_shop_invalidations_total %d\n", s.invalidations.Load()) + fmt.Sprintf("xzdp_shop_db_loads_total{kind=\"detail\"} %d\nxzdp_shop_db_loads_total{kind=\"types\"} %d\nxzdp_shop_bloom_rejections_total %d\nxzdp_shop_existence_probes_total %d\n", s.loads[0].Load(), s.loads[1].Load(), s.rejected.Load(), s.existenceProbes.Load())
}

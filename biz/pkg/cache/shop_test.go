package cache

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"xzdp/biz/model/shop"
	"xzdp/conf"
)

func fixture(t *testing.T) (*miniredis.Miniredis, *ShopCache) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	cfg := conf.DefaultShopCache()
	cfg.MemoryMiB = 1
	cfg.HotIDs = nil
	source := ShopSource{
		Detail: func(ctx context.Context, id int64) (*shop.Shop, error) { return &shop.Shop{ID: id}, nil },
		IDs:    func(context.Context) ([]int64, error) { return []int64{1}, nil },
		Exists: func(_ context.Context, id int64) (bool, error) { return id == 2, nil },
		Types:  func(context.Context) ([]*shop.ShopType, error) { return []*shop.ShopType{{ID: 1, Name: "美食"}}, nil },
	}
	return mr, NewShopCache("test", cfg, rdb, source)
}

func TestTwoNodesShareFillAndInvalidate(t *testing.T) {
	_, a := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	var version atomic.Int64
	var queries atomic.Int64
	a.source.Detail = func(_ context.Context, id int64) (*shop.Shop, error) {
		queries.Add(1)
		time.Sleep(25 * time.Millisecond)
		return &shop.Shop{ID: id, Sold: int32(version.Load())}, nil
	}
	b := NewShopCache("test", a.cfg, a.redis, a.source)
	nodes := []*ShopCache{a, b}
	var subscribers sync.WaitGroup
	defer func() { cancel(); subscribers.Wait() }()
	for _, node := range nodes {
		subscribers.Add(1)
		go func(n *ShopCache) { defer subscribers.Done(); n.Subscribe(ctx) }(node)
		select {
		case <-node.rebuild:
		case <-time.After(time.Second):
			t.Fatal("subscription not ready")
		}
	}
	gate := make(chan struct{})
	errs := make(chan error, 500)
	for i := 0; i < 500; i++ {
		go func(n *ShopCache) {
			<-gate
			v, err := n.Detail(ctx, 1)
			if err == nil && v.ID != 1 {
				err = errors.New("wrong shop")
			}
			errs <- err
		}(nodes[i%2])
	}
	close(gate)
	for i := 0; i < 500; i++ {
		require.NoError(t, <-errs)
	}
	require.EqualValues(t, 1, queries.Load())
	version.Store(2)
	require.NoError(t, a.Invalidate(ctx, 1))
	require.Eventually(t, func() bool { return a.invalidations.Load() > 0 && b.invalidations.Load() > 0 }, time.Second, time.Millisecond)
	for _, n := range nodes {
		v, err := n.Detail(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, v.Sold)
	}
	require.EqualValues(t, 2, queries.Load())
}

func TestBloomNegativeCacheAndNewShop(t *testing.T) {
	mr, s := fixture(t)
	ctx := context.Background()
	require.NoError(t, s.Refresh(ctx))
	_, err := s.Detail(ctx, 99)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	_, err = s.Detail(ctx, 99)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	require.EqualValues(t, 1, s.existenceProbes.Load())
	require.Equal(t, 3*time.Second, mr.TTL(s.prefix+"detail:99"))
	// ID 2 was added after the Bloom snapshot.
	v, err := s.Detail(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, v.ID)
	ttl := mr.TTL(s.prefix + "detail:2")
	require.GreaterOrEqual(t, ttl, 480*time.Second)
	require.LessOrEqual(t, ttl, 720*time.Second)
	types, err := s.Types(ctx)
	require.NoError(t, err)
	require.Len(t, types, 1)
	require.EqualValues(t, 1, s.loads[1].Load())
}

func TestRedisOfflineAndCallerCancellation(t *testing.T) {
	mr, s := fixture(t)
	mr.Close()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	s.source.Detail = func(ctx context.Context, id int64) (*shop.Shop, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return &shop.Shop{ID: id}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Detail(ctx, 1); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("DB fallback did not start")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)
	v, err := s.Detail(context.Background(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, v.ID)
	require.EqualValues(t, 1, calls.Load())
}

func TestInvalidationFencesInflightWrite(t *testing.T) {
	_, s := fixture(t)
	ctx := context.Background()
	var calls atomic.Int64
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.source.Detail = func(context.Context, int64) (*shop.Shop, error) {
		version := calls.Add(1)
		if version == 1 {
			close(started)
			<-release
		}
		return &shop.Shop{ID: 1, Sold: int32(version)}, nil
	}
	go func() { defer close(finished); s.Detail(ctx, 1) }()
	<-started
	require.NoError(t, s.Invalidate(ctx, 1))
	next, err := s.Detail(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Sold)
	close(release)
	<-finished
	s.local.Clear()
	next, err = s.Detail(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Sold)
	require.EqualValues(t, 2, calls.Load())
}

func TestReconnectDropsMissedL1Entry(t *testing.T) {
	mr, s := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Subscribe(ctx) }()
	defer func() { cancel(); <-done }()
	waitSubscription := func() {
		t.Helper()
		select {
		case <-s.rebuild:
		case <-time.After(8 * time.Second):
			t.Fatal("missing subscription confirmation")
		}
	}
	waitSubscription()
	_, err := s.Detail(ctx, 5)
	require.NoError(t, err)
	key := s.prefix + "detail:5"
	_, err = s.local.Get([]byte(key))
	require.NoError(t, err)
	mr.Close()
	require.NoError(t, mr.Restart())
	waitSubscription()
	_, err = s.local.Get([]byte(key))
	require.Error(t, err)
}

func TestUnresponsiveRedisLeavesDatabaseTime(t *testing.T) {
	_, cache := fixture(t)
	slow := redis.NewClient(&redis.Options{MaxRetries: -1, Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	defer slow.Close()
	cache.redis = slow
	cache.source.Detail = func(ctx context.Context, id int64) (*shop.Shop, error) { return &shop.Shop{ID: id}, ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := cache.Detail(ctx, 4)
	require.NoError(t, err)
	require.EqualValues(t, 4, v.ID)
}

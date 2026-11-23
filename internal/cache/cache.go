// Package cache 提供商铺详情的多级缓存：进程内 LRU + Redis + singleflight 回源合并 + 逻辑过期刷新，空值短缓存防穿透。
package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type entry struct {
	Data           json.RawMessage `json:",omitempty"`
	Fresh, Expires time.Time
}
type item struct {
	key   string
	entry entry
}
type Cache struct {
	r             *redis.Client
	prefix        string
	capacity      int
	ttl           time.Duration
	mu            sync.Mutex
	queue         *list.List
	index         map[string]*list.Element
	group         singleflight.Group
	L1, L2, Loads atomic.Uint64
}

func New(r *redis.Client, prefix string, capacity int, ttl time.Duration) *Cache {
	return &Cache{r: r, prefix: prefix, capacity: capacity, ttl: ttl, queue: list.New(), index: make(map[string]*list.Element)}
}
func (c *Cache) local(k string) (entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.index[k]; elem != nil {
		e := elem.Value.(item).entry
		if time.Now().Before(e.Expires) {
			c.queue.MoveToFront(elem)
			return e, true
		}
		delete(c.index, k)
		c.queue.Remove(elem)
	}
	return entry{}, false
}
func (c *Cache) put(k string, e entry) {
	if c.capacity <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.index[k]; old != nil {
		c.queue.Remove(old)
	}
	c.index[k] = c.queue.PushFront(item{k, e})
	if c.queue.Len() > c.capacity {
		old := c.queue.Back()
		delete(c.index, old.Value.(item).key)
		c.queue.Remove(old)
	}
}

type Loader func(context.Context) (json.RawMessage, error)

func (c *Cache) Get(ctx context.Context, k string, load Loader) (json.RawMessage, error) {
	key := c.prefix + k
	fetch := func() (any, error) {
		// A request can reach the group just after another loader publishes.
		if e, ok := c.local(k); ok && time.Now().Before(e.Fresh) {
			return e, nil
		}
		work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 4*time.Second)
		defer cancel()
		b, err := c.r.Get(work, key).Bytes()
		var v entry
		if err == nil && json.Unmarshal(b, &v) == nil && time.Now().Before(v.Fresh) {
			if string(v.Data) == "null" {
				v.Data = nil
			}
			c.L2.Add(1)
			c.put(k, v)
			return v, nil
		}
		c.Loads.Add(1)
		data, err := load(work)
		if err != nil {
			return nil, err
		}
		ttl := c.ttl
		retention := 2 * ttl
		if len(data) == 0 {
			ttl = min(ttl, 15*time.Second)
			retention = ttl
		}
		now := time.Now()
		v = entry{data, now.Add(ttl), now.Add(retention)}
		b, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
		c.r.Set(work, key, b, retention)
		c.put(k, v)
		return v, nil
	}
	if e, ok := c.local(k); ok {
		c.L1.Add(1)
		if time.Now().After(e.Fresh) {
			c.group.DoChan(k, fetch)
		}
		return e.Data, nil
	}
	ch := c.group.DoChan(k, fetch)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		v, ok := r.Val.(entry)
		if !ok {
			return nil, errors.New("invalid cache entry")
		}
		return v.Data, nil
	}
}

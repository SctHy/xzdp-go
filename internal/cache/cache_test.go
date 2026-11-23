package cache

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEvictionAndExpiry(t *testing.T) {
	c := New(nil, "", 2, time.Minute)
	live := entry{Expires: time.Now().Add(time.Minute)}
	c.put("a", live)
	c.put("b", live)
	c.local("a")
	c.put("c", live)
	if _, ok := c.local("b"); ok {
		t.Fatal("evicted a recently used record instead of b")
	}
	c.put("a", entry{Expires: time.Now().Add(-time.Second)})
	if _, ok := c.local("a"); ok {
		t.Fatal("expired record served")
	}
	if c.queue.Len() != 1 {
		t.Fatal("stale LRU node")
	}
}
func TestMissSurvivesL2Encoding(t *testing.T) {
	b, e := json.Marshal(entry{})
	if e != nil {
		t.Fatal(e)
	}
	var v entry
	if e = json.Unmarshal(b, &v); e != nil || len(v.Data) != 0 {
		t.Fatalf("negative record became a positive JSON value: %s", b)
	}
}

func testCache(t *testing.T) (*Cache, *miniredis.Miniredis) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	t.Cleanup(func() { client.Close() })
	return New(client, "shop:", 2, time.Minute), r
}

func TestNegativeCache(t *testing.T) {
	c, r := testCache(t)
	calls := 0
	load := func(context.Context) (json.RawMessage, error) { calls++; return nil, nil }
	for range 2 {
		data, err := c.Get(t.Context(), "missing", load)
		if err != nil || data != nil {
			t.Fatal(data, err)
		}
	}
	if ttl := r.TTL("shop:missing"); ttl != 15*time.Second {
		t.Fatalf("negative TTL %s", ttl)
	}
	// A fresh process should also serve the negative entry from Redis.
	fresh := New(c.r, c.prefix, 2, time.Minute)
	if data, err := fresh.Get(t.Context(), "missing", load); err != nil || data != nil || calls != 1 || fresh.L2.Load() != 1 || c.L1.Load() != 1 {
		t.Fatal(data, err, calls)
	}
}

func TestStaleReturnsDuringRefresh(t *testing.T) {
	c, _ := testCache(t)
	c.put("k", entry{json.RawMessage("1"), time.Now().Add(-time.Second), time.Now().Add(time.Minute)})
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	load := func(ctx context.Context) (json.RawMessage, error) {
		close(started)
		select {
		case <-release:
			return json.RawMessage("2"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	data, err := c.Get(ctx, "k", load)
	if err != nil || string(data) != "1" {
		t.Fatal(string(data), err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("refresh never started")
	}
	unblock()
	// Wait for publication before checking the next L1 hit.
	for {
		e, _ := c.local("k")
		if string(e.Data) == "2" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("refreshed value never published")
		case <-time.After(time.Millisecond):
		}
	}
	if c.Loads.Load() != 1 {
		t.Fatal(c.Loads.Load())
	}
}

// Get selects on Done after joining the singleflight group.
type joinedContext struct {
	context.Context
	joined chan<- struct{}
}

func (c joinedContext) Done() <-chan struct{} { c.joined <- struct{}{}; return c.Context.Done() }

func TestConcurrentLoadsCoalesce(t *testing.T) {
	c, _ := testCache(t)
	const n = 16
	joined := make(chan struct{}, n)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			data, err := c.Get(joinedContext{ctx, joined}, "k", func(context.Context) (json.RawMessage, error) { <-release; return json.RawMessage("42"), nil })
			if err != nil || string(data) != "42" {
				t.Errorf("Get = %s, %v", data, err)
			}
		})
	}
	for range n {
		select {
		case <-joined:
		case <-ctx.Done():
			unblock()
			wg.Wait()
			t.Fatal("callers did not join")
		}
	}
	unblock()
	wg.Wait()
	if c.Loads.Load() != 1 {
		t.Fatalf("%d loads for %d callers", c.Loads.Load(), n)
	}
}

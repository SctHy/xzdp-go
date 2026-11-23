// Command bench creates and drops a dedicated database and Redis key namespace per run.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"xzdp/internal/cache"
	"xzdp/internal/mq"
	"xzdp/internal/shop"
	"xzdp/internal/store"
	"xzdp/internal/trade"
)

type timing struct {
	Requests, Concurrency int
	QPS, P50ms, P99ms     float64
}

func measure(n, workers int, fn func(int) error) (timing, error) {
	return perWorker(n, workers, func(_ int, i int) error { return fn(i) })
}
func perWorker(n, workers int, fn func(int, int) error) (timing, error) {
	var next atomic.Int64
	var wg sync.WaitGroup
	dur := make([]float64, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for worker := range workers {
		wg.Go(func() {
			<-start
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				t := time.Now()
				errs[i] = fn(worker, i)
				dur[i] = float64(time.Since(t)) / float64(time.Millisecond)
			}
		})
	}
	t := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t)
	slices.Sort(dur)
	return timing{n, workers, float64(n) / elapsed.Seconds(), dur[int(math.Ceil(float64(n)*0.5))-1], dur[int(math.Ceil(float64(n)*0.99))-1]}, errors.Join(errs...)
}
func fixture(ctx context.Context, fn func(*store.Store) error) (err error) {
	uri, e := url.Parse(store.Env("DATABASE_URL", "postgres://xzdp:xzdp@localhost:5432/xzdp?sslmode=disable"))
	if e != nil {
		return e
	}
	admin, e := pgx.Connect(ctx, uri.String())
	if e != nil {
		return e
	}
	defer admin.Close(context.Background())
	name := "xzdp_bench_" + strings.ToLower(rand.Text())
	ident := pgx.Identifier{name}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+ident); e != nil {
		return e
	}
	defer func() { _, e := admin.Exec(context.Background(), "DROP DATABASE "+ident); err = errors.Join(err, e) }()
	uri.Path = "/" + name
	pool, e := pgxpool.New(ctx, uri.String())
	if e != nil {
		return e
	}
	defer pool.Close()
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	opts, e := redis.ParseURL(store.Env("REDIS_URL", "redis://localhost:6379/0"))
	if e != nil {
		return e
	}
	r := redis.NewClient(opts)
	defer r.Close()
	s := &store.Store{Pool: pool, DB: db, Redis: r, Prefix: name + ":"}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var cursor uint64
		var keys []string
		for {
			batch, next, e := r.Scan(ctx, cursor, s.Prefix+"*", 256).Result()
			if e != nil {
				err = errors.Join(err, e)
				return
			}
			keys = append(keys, batch...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(keys) > 0 {
			err = errors.Join(err, r.Del(ctx, keys...).Err())
		}
	}()
	if e = store.Install(ctx, db, true); e != nil {
		return e
	}
	return fn(s)
}
func caches(ctx context.Context, s *store.Store, out map[string]any) error {
	prefix := s.Prefix + "shop:"
	// Seed and explicitly invalidate the L2 record before releasing cold readers.
	seed := cache.New(s.Redis, prefix, 0, time.Minute)
	if _, e := shop.Detail(ctx, s.DB, seed, 1); e != nil {
		return e
	}
	if e := s.Redis.Del(ctx, prefix+"1").Err(); e != nil {
		return e
	}
	for _, stage := range []string{"cold", "L1", "Redis"} {
		n, w := 5000, 64
		if stage == "cold" {
			n, w = 200, 200
		}
		instances := []*cache.Cache{cache.New(s.Redis, prefix, 256, time.Minute)}
		if stage == "Redis" {
			// Independent API instances model simultaneous L1 misses with a warm L2.
			instances = make([]*cache.Cache, w)
			for i := range instances {
				instances[i] = cache.New(s.Redis, prefix, 0, time.Minute)
			}
		}
		if stage == "L1" {
			if _, e := shop.Detail(ctx, s.DB, instances[0], 1); e != nil {
				return e
			}
		}
		stats, e := perWorker(n, w, func(worker, i int) error {
			_, e := shop.Detail(ctx, s.DB, instances[worker%len(instances)], 1)
			return e
		})
		if e != nil {
			return e
		}
		var loads, l1, l2 uint64
		for _, c := range instances {
			loads += c.Loads.Load()
			l1 += c.L1.Load()
			l2 += c.L2.Load()
		}
		if stage == "L1" {
			l2--
		} // exclude setup
		out["cache_"+stage] = map[string]any{"timing": stats, "database_loads": loads, "l1_hits": l1, "redis_hits": l2}
		if stage == "cold" && loads != 1 || stage != "cold" && loads != 0 || stage == "Redis" && l2 != uint64(n) {
			return fmt.Errorf("unexpected cache counts for %s", stage)
		}
	}
	return nil
}

// Use dedicated topics even when the broker is shared with other projects.
func benchQueue(ctx context.Context, prefix string) (*mq.Queue, func() error, error) {
	topic := strings.TrimSuffix(prefix, ":") + ".orders"
	admin, err := mq.Open(topic, false)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error {
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic, topic + ".dlq"}
		for _, name := range req.TopicNames {
			t := kmsg.NewDeleteTopicsRequestTopic()
			t.Topic = &name
			req.Topics = append(req.Topics, t)
		}
		res, err := req.RequestWith(ctx, admin.Client)
		if err != nil {
			return err
		}
		for _, t := range res.Topics {
			if e := kerr.ErrorForCode(t.ErrorCode); e != nil && e != kerr.UnknownTopicOrPartition {
				return e
			}
		}
		return nil
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range []string{topic, topic + ".dlq"} {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic = name
		t.NumPartitions = 4
		t.ReplicationFactor = 1
		req.Topics = append(req.Topics, t)
	}
	res, err := req.RequestWith(ctx, admin.Client)
	if err == nil {
		for _, t := range res.Topics {
			err = errors.Join(err, kerr.ErrorForCode(t.ErrorCode))
		}
	}
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	q, err := mq.Open(topic, true)
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	return q, func() error { q.CloseAllowingRebalance(); return cleanup() }, nil
}

type crashPublisher struct {
	queue *mq.Queue
	calls atomic.Int64
}

func (p *crashPublisher) Publish(ctx context.Context, id string, value []byte) error {
	if p.calls.Add(1) <= 10 {
		return errors.New("simulated interruption before Kafka send")
	}
	return p.queue.Publish(ctx, id, value)
}

func sales(ctx context.Context, s *store.Store, b int, out map[string]any) (err error) {
	v := int64(100 + b)
	_, e := s.DB.ExecContext(ctx, "INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES($1,100,now()-interval '1 minute',now()+interval '1 hour')", v)
	if e != nil {
		return e
	}
	sale := trade.Sale{DB: s.DB, Redis: s.Redis, Prefix: fmt.Sprintf("%sv%d:", s.Prefix, b), Buckets: b}
	q, cleanup, e := benchQueue(ctx, fmt.Sprintf("%sv%d", strings.TrimSuffix(s.Prefix, ":"), b))
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, cleanup()) }()
	sale.Queue = q
	sale.Publisher = &crashPublisher{queue: q}
	if e = sale.Warm(ctx, v); e != nil {
		return e
	}
	if _, e := measure(200, 200, func(int) error { return s.Redis.Ping(ctx).Err() }); e != nil {
		return e
	}
	ids := make([]string, 200)
	start := time.Now()
	stats, e := measure(200, 200, func(i int) error {
		id, e := sale.Reserve(ctx, v, int64(i+1))
		if errors.Is(e, trade.SoldOut) {
			return nil
		}
		ids[i] = id
		return e
	})
	if e != nil {
		return e
	}
	accepted := 0
	for i, id := range ids {
		if id == "" {
			continue
		}
		accepted++
		again, e := sale.Reserve(ctx, v, int64(i+1))
		if e != nil || again != id {
			return fmt.Errorf("buyer retry changed order: %v", e)
		}
	}
	if accepted != 100 {
		return fmt.Errorf("accepted %d", accepted)
	}
	abandoned := 10
	normal := 0
	for normal < accepted-abandoned {
		n, e := sale.Read(ctx)
		if e != nil {
			return e
		}
		normal += n
	}
	missing, e := s.Redis.ZCard(ctx, sale.Pending()).Result()
	if e != nil {
		return e
	}
	if missing != 10 {
		return fmt.Errorf("expected ten unpublished reservations, found %d", missing)
	}
	sale.ReconcileAge = 100 * time.Millisecond
	if e = waitForRecovery(ctx, sale); e != nil {
		return e
	}
	var count, unique, left int
	if e = s.DB.QueryRowContext(ctx, "SELECT count(*),count(DISTINCT user_id),(SELECT stock FROM tb_seckill_voucher WHERE voucher_id=$1) FROM tb_voucher_order WHERE voucher_id=$1", v).Scan(&count, &unique, &left); e != nil {
		return e
	}
	pending, e := s.Redis.ZCard(ctx, sale.Pending()).Result()
	if e != nil {
		return e
	}
	rows, e := s.DB.QueryContext(ctx, "SELECT id::text,user_id FROM tb_voucher_order WHERE voucher_id=$1", v)
	if e != nil {
		return e
	}
	for rows.Next() {
		var id string
		var u int
		if e = rows.Scan(&id, &u); e != nil {
			rows.Close()
			return e
		}
		if u < 1 || u > len(ids) || ids[u-1] != id {
			rows.Close()
			return errors.New("persisted identity differs from admission")
		}
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	claimed := count - normal
	if abandoned != 10 || count != 100 || unique != count || left != 0 || claimed != abandoned || pending != 0 {
		return fmt.Errorf("inconsistent orders %d/%d stock %d recovered %d pending %d", count, unique, left, claimed, pending)
	}
	out[fmt.Sprintf("sale_%d_bucket", b)] = map[string]any{"admission": stats, "accepted": accepted, "persisted": count, "stock_remaining": left, "oversold": max(count-100, 0), "lost": accepted - count, "duplicate_orders": count - unique, "abandoned": abandoned, "reconciled": claimed, "pending": pending, "end_to_end_ms_including_retries": time.Since(start).Milliseconds()}
	return nil
}

// Drive the production Worker until it has committed every pending reservation.
func waitForRecovery(ctx context.Context, sale trade.Sale) error {
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var workerErr error
	go func() { defer close(done); workerErr = sale.Run(run) }()
	defer func() { cancel(); <-done }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return workerErr
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := sale.Redis.ZCard(ctx, sale.Pending()).Result()
			if err != nil || n == 0 {
				return err
			}
		}
	}
}

func geo(ctx context.Context, s *store.Store, out map[string]any) error {
	_, e := s.DB.ExecContext(ctx, `INSERT INTO tb_shop(name,type_id,images,address,x,y,sold,comments,score) SELECT 'Bench cafe '||i,1,'','',120.1+(i%100)::float/1000,30.2+(i/100)::float/1000,0,0,40 FROM generate_series(1,10000)i`)
	if e != nil {
		return e
	}
	if _, e = s.DB.ExecContext(ctx, "ANALYZE tb_shop"); e != nil {
		return e
	}
	stats, e := measure(500, 20, func(i int) error {
		rows, e := shop.Search(ctx, s.DB, shop.Query{X: 120.14 + float64(i%10)/1000, Y: 30.25, Radius: 1200, Q: "cafe", Type: 1})
		if e != nil {
			return e
		}
		defer rows.Close()
		n := 0
		prev := -1.
		for rows.Next() {
			var h shop.Hit
			if e = rows.Scan(&h.ID, &h.Name, &h.Type, &h.Distance); e != nil {
				return e
			}
			if h.Distance < prev || h.Distance > 1200 {
				return errors.New("invalid distance")
			}
			prev = h.Distance
			n++
		}
		if n != 10 {
			return fmt.Errorf("got %d shops", n)
		}
		return rows.Err()
	})
	out["nearby"] = map[string]any{"inserted": 10000, "timing": stats}
	return e
}
func run(ctx context.Context, w io.Writer) error {
	out := map[string]any{"utc": time.Now().UTC(), "go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH}
	e := fixture(ctx, func(s *store.Store) error {
		var pg, spatial string
		if e := s.DB.QueryRowContext(ctx, "SELECT current_setting('server_version'),postgis_lib_version()").Scan(&pg, &spatial); e != nil {
			return e
		}
		info, e := s.Redis.Info(ctx, "server").Result()
		if e != nil {
			return e
		}
		for _, line := range strings.Split(info, "\r\n") {
			if version, ok := strings.CutPrefix(line, "redis_version:"); ok {
				out["redis_version"] = version
			}
		}
		out["postgres_version"], out["postgis_version"] = pg, spatial
		out["scope"] = "Direct service calls from the host to Redis, Kafka and PostGIS. Sale latency measures admission; end-to-end includes duplicate retries, persistence and reconciliation."
		if e := caches(ctx, s, out); e != nil {
			return e
		}
		for _, b := range []int{1, 8} {
			if e := sales(ctx, s, b, out); e != nil {
				return e
			}
		}
		return geo(ctx, s, out)
	})
	if e != nil {
		return e
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
func main() {
	dest := flag.String("json", "", "write verified measurements to file")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var b strings.Builder
	e := run(ctx, &b)
	if e == nil {
		if *dest != "" {
			e = os.WriteFile(*dest, []byte(b.String()), 0644)
		} else {
			fmt.Print(b.String())
		}
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

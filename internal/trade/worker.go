package trade

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Permanent contradictions require operator inspection. Connectivity and transaction errors stay pending.
type rejected string

func (r rejected) Error() string { return string(r) }

type event struct {
	id, user, voucher int64
	bucket            int
}

func parse(m map[string]string) (event, error) {
	var evt event
	for key, dst := range map[string]*int64{"id": &evt.id, "user": &evt.user, "voucher": &evt.voucher} {
		v, e := strconv.ParseInt(m[key], 10, 64)
		if e != nil || v < 1 {
			return evt, fmt.Errorf("invalid %s", key)
		}
		*dst = v
	}
	b, e := strconv.Atoi(m["bucket"])
	if e != nil || b < 0 || b >= 64 {
		return evt, errors.New("invalid bucket")
	}
	evt.bucket = b
	return evt, nil
}
func (s Sale) persist(ctx context.Context, v event) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, e := tx.ExecContext(ctx, "INSERT INTO tb_voucher_order(id,user_id,voucher_id,bucket) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING", v.id, v.user, v.voucher, v.bucket)
	if e != nil {
		return e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return e
	}
	if n == 1 {
		var left int
		e = tx.QueryRowContext(ctx, "UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=$1 AND stock>0 RETURNING stock", v.voucher).Scan(&left)
		if errors.Is(e, sql.ErrNoRows) {
			return rejected("database stock exhausted or missing")
		}
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO trade_notice(event_key,user_id,content) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", fmt.Sprintf("order:%d", v.id), v.user, fmt.Sprintf("优惠券订单 %d 已生成", v.id)); e != nil {
			return e
		}
	} else {
		var same bool
		if e = tx.QueryRowContext(ctx, "SELECT user_id=$1 AND voucher_id=$2 FROM tb_voucher_order WHERE id=$3", v.user, v.voucher, v.id).Scan(&same); e != nil {
			return e
		}
		if !same {
			return rejected("order identity collision")
		}
	}
	return tx.Commit()
}

var finishMessage = redis.NewScript(`
if redis.call('HGET',KEYS[2],'payload')~=ARGV[2] then return 0 end
redis.call('DEL',KEYS[2])
return redis.call('ZREM',KEYS[1],ARGV[1])`)

func (s Sale) consume(ctx context.Context, r *kgo.Record) error {
	var fields map[string]string
	var v event
	bad := json.Unmarshal(r.Value, &fields)
	if bad == nil {
		v, bad = parse(fields)
	}
	valid := bad == nil
	if valid {
		err := s.persist(ctx, v)
		var permanent rejected
		var pg *pgconn.PgError
		switch {
		case errors.As(err, &permanent):
			bad = permanent
		case errors.As(err, &pg) && pg.Code == "23505":
			bad = rejected("conflicting buyer reservation")
		case err != nil:
			return err
		}
	}
	if bad != nil {
		source := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
		var id any
		if valid {
			id = v.id
		}
		_, err := s.DB.ExecContext(ctx, "INSERT INTO trade_dead_letter(order_id,voucher_id,user_id,reason,source,payload) VALUES($1,$2,$3,left($4,255),$5,$6) ON CONFLICT DO NOTHING", id, v.voucher, v.user, bad.Error(), source, r.Value)
		if err != nil {
			return err
		}
		err = s.Queue.ProduceSync(ctx, &kgo.Record{Topic: s.Queue.Topic + ".dlq", Key: []byte(source), Value: r.Value, Headers: []kgo.RecordHeader{{Key: "reason", Value: []byte(bad.Error())}}}).FirstErr()
		if err != nil {
			return err
		}
		s.Metrics.MQ("dead_letter", 1)
	}
	if valid {
		id := strconv.FormatInt(v.id, 10)
		if err := finishMessage.Run(ctx, s.Redis, []string{s.Pending(), s.Prefix + "queued:" + id}, id, r.Value).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Rewind failed batches before releasing the partitions for a rebalance.
func (s Sale) Read(ctx context.Context) (n int, err error) {
	f := s.Queue.PollRecords(ctx, 64)
	defer s.Queue.AllowRebalance()
	batch, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	records := f.Records()
	defer func() {
		if err == nil || len(records) == 0 {
			return
		}
		offsets := map[int32]kgo.EpochOffset{}
		for _, r := range records {
			if _, ok := offsets[r.Partition]; !ok {
				offsets[r.Partition] = kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset}
			}
		}
		s.Queue.SetOffsets(map[string]map[int32]kgo.EpochOffset{s.Queue.Topic: offsets})
	}()
	for i, r := range records {
		if err := s.consume(batch, r); err != nil {
			return i, err
		}
	}
	if len(records) > 0 {
		if err := s.Queue.CommitRecords(batch, records...); err != nil {
			return 0, err
		}
		s.Metrics.MQ("consume", len(records))
		return len(records), nil
	}
	return 0, f.Err()
}

func (s Sale) Reconcile(ctx context.Context, age time.Duration) (int, error) {
	now, err := s.Redis.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	ids, err := s.Redis.ZRangeByScore(ctx, s.Pending(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.Add(-age).UnixMilli(), 10), Count: 256}).Result()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		payload, err := s.Redis.HGet(ctx, s.Prefix+"queued:"+id, "payload").Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return n, err
		}
		if err = s.Queue.Publish(ctx, id, payload); err != nil {
			return n, err
		}
		if err = s.Redis.ZAddXX(ctx, s.Pending(), redis.Z{Score: float64(now.UnixMilli()), Member: id}).Err(); err != nil {
			return n, err
		}
		n++
		s.Metrics.MQ("reconcile", 1)
	}
	return n, nil
}
func (s Sale) Expire(ctx context.Context) error {
	_, e := s.DB.ExecContext(ctx, `WITH locked AS (SELECT id FROM tb_voucher_order WHERE status=1 AND create_time<=now()-interval '15 minutes' ORDER BY create_time LIMIT 100 FOR UPDATE SKIP LOCKED),
closed AS (UPDATE tb_voucher_order o SET status=4 FROM locked WHERE o.id=locked.id RETURNING o.id,o.voucher_id),
queued AS (INSERT INTO trade_release(order_id,voucher_id) SELECT id,voucher_id FROM closed RETURNING voucher_id)
UPDATE tb_seckill_voucher s SET stock=s.stock+q.n FROM (SELECT voucher_id,count(*) n FROM queued GROUP BY voucher_id) q WHERE s.voucher_id=q.voucher_id`)
	if e != nil {
		return e
	}
	rows, e := s.DB.QueryContext(ctx, "SELECT r.order_id,r.voucher_id,o.bucket FROM trade_release r JOIN tb_voucher_order o ON o.id=r.order_id ORDER BY r.order_id LIMIT 100")
	if e != nil {
		return e
	}
	defer rows.Close()
	var releases [][3]int64
	for rows.Next() {
		var r [3]int64
		if e = rows.Scan(&r[0], &r[1], &r[2]); e != nil {
			return e
		}
		releases = append(releases, r)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	for _, r := range releases {
		if e = s.Warm(ctx, r[1]); e != nil {
			return e
		}
		k := s.key(r[1])
		id := newOrderID()
		payload, err := releaseScript.Run(ctx, s.Redis, []string{k, k + ":released", k + ":wait", k + ":buyers", s.Pending(), s.Prefix + "queued:" + id}, r[0], r[2], r[1], id).Text()
		if err != nil {
			return err
		}
		if payload != "" {
			s.publishAdmission(ctx, id, payload)
		}
		if _, e = s.DB.ExecContext(ctx, "DELETE FROM trade_release WHERE order_id=$1", r[0]); e != nil {
			return e
		}
	}
	return nil
}
func (s Sale) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() {
		defer close(done)
		ticker := time.NewTicker(min(s.ReconcileAge, time.Second))
		defer ticker.Stop()
		var next time.Time
		for ctx.Err() == nil {
			if _, err := s.Reconcile(ctx, s.ReconcileAge); err != nil && ctx.Err() == nil {
				slog.Error("reconcile", "error", err)
			}
			if time.Now().After(next) {
				if err := s.Expire(ctx); err != nil {
					slog.Error("expire", "error", err)
				}
				if err := s.reminders(ctx); err != nil {
					slog.Error("reminders", "error", err)
				}
				next = time.Now().Add(5 * time.Second)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	for ctx.Err() == nil {
		if _, err := s.Read(ctx); err != nil && ctx.Err() == nil {
			slog.Error("consume", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}
func (s Sale) Status(ctx context.Context, id, u int64) (string, error) {
	// Check queued first. The worker removes this marker only after SQL commit.
	owner, e := s.Redis.HGet(ctx, fmt.Sprintf("%squeued:%d", s.Prefix, id), "user").Int64()
	if e == nil && owner == u {
		return "queued", nil
	}
	if e != nil && !errors.Is(e, redis.Nil) {
		return "", e
	}
	var status string
	e = s.DB.QueryRowContext(ctx, `SELECT CASE status WHEN 1 THEN 'unpaid' WHEN 2 THEN 'paid' WHEN 3 THEN 'used' WHEN 4 THEN 'cancelled' END FROM tb_voucher_order WHERE id=$1 AND user_id=$2 UNION ALL SELECT 'failed' FROM trade_dead_letter WHERE order_id=$1 AND user_id=$2 LIMIT 1`, id, u).Scan(&status)
	return status, e
}

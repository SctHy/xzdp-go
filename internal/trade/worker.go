package trade

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
)

func (s Sale) InitGroup(ctx context.Context) error {
	e := s.Redis.XGroupCreateMkStream(ctx, s.Stream(), Group, "0").Err()
	if e != nil && strings.HasPrefix(e.Error(), "BUSYGROUP") {
		return nil
	}
	return e
}

// Permanent contradictions require operator inspection. Connectivity and transaction errors stay pending.
type rejected string

func (r rejected) Error() string { return string(r) }

type event struct {
	id, user, voucher int64
	bucket            int
}

func parse(m redis.XMessage) (event, error) {
	var evt event
	for key, dst := range map[string]*int64{"id": &evt.id, "user": &evt.user, "voucher": &evt.voucher} {
		v, e := strconv.ParseInt(fmt.Sprint(m.Values[key]), 10, 64)
		if e != nil || v < 1 {
			return evt, fmt.Errorf("invalid %s", key)
		}
		*dst = v
	}
	b, e := strconv.Atoi(fmt.Sprint(m.Values["bucket"]))
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

// A slow original consumer and a claimant can both finish. Publish at most one
// dead letter, and leave the original pending if writing that letter fails.
var finishMessage = redis.NewScript(`
if #redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)==0 then return 0 end
if ARGV[3]~='' then
 redis.call('XADD',KEYS[2],'*','source',ARGV[2],'reason',ARGV[3],'payload',ARGV[4])
end
if ARGV[5]~='0' then redis.call('DEL',KEYS[3]) end
redis.call('XACK',KEYS[1],ARGV[1],ARGV[2])
redis.call('XDEL',KEYS[1],ARGV[2])
return 1`)

func (s Sale) consume(ctx context.Context, msgs []redis.XMessage) (int, error) {
	for i, m := range msgs {
		v, bad := parse(m)
		if bad == nil {
			if e := s.persist(ctx, v); e != nil {
				var reason rejected
				var constraint *pgconn.PgError
				switch {
				case errors.As(e, &reason):
					bad = reason
				case errors.As(e, &constraint) && constraint.Code == "23505":
					bad = rejected("conflicting buyer reservation")
				default:
					return i, e
				}
				if _, e = s.DB.ExecContext(ctx, "INSERT INTO trade_dead_letter(order_id,voucher_id,user_id,reason) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", v.id, v.voucher, v.user, bad.Error()); e != nil {
					return i, e
				}
			}
		}
		reason := ""
		if bad != nil {
			reason = bad.Error()
		}
		changed, e := finishMessage.Run(ctx, s.Redis, []string{s.Stream(), s.Stream() + ":dead", s.Prefix + "queued:" + strconv.FormatInt(v.id, 10)}, Group, m.ID, reason, fmt.Sprint(m.Values), v.id).Int()
		if e != nil {
			return i, e
		}
		if changed == 0 {
			continue
		}
		s.Metrics.Stream("ack", 1)
		if bad != nil {
			s.Metrics.Stream("dead_letter", 1)
		}
	}
	return len(msgs), nil
}
func (s Sale) Read(ctx context.Context, name string) (int, error) {
	v, e := s.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{Group: Group, Consumer: name, Streams: []string{s.Stream(), ">"}, Count: 64, Block: time.Second}).Result()
	if errors.Is(e, redis.Nil) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	return s.consume(ctx, v[0].Messages)
}
func (s Sale) Claim(ctx context.Context, name string, idle time.Duration) (int, error) {
	cursor := "0-0"
	total := 0
	for {
		v, next, e := s.Redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{Group: Group, Consumer: name, Stream: s.Stream(), Start: cursor, MinIdle: idle, Count: 64}).Result()
		if e != nil {
			return total, e
		}
		n, e := s.consume(ctx, v)
		total += n
		s.Metrics.Stream("reclaim", n)
		if e != nil || next == "0-0" {
			return total, e
		}
		cursor = next
	}
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
		if e = releaseScript.Run(ctx, s.Redis, []string{k, k + ":released", k + ":wait", k + ":buyers", s.Stream(), s.Prefix + "queued:" + id}, r[0], r[2], r[1], id).Err(); e != nil {
			return e
		}
		if _, e = s.DB.ExecContext(ctx, "DELETE FROM trade_release WHERE order_id=$1", r[0]); e != nil {
			return e
		}
	}
	return nil
}
func (s Sale) Run(ctx context.Context, name string) error {
	if e := s.InitGroup(ctx); e != nil {
		return e
	}
	var next time.Time
	for ctx.Err() == nil {
		if time.Now().After(next) {
			if _, e := s.Claim(ctx, name, 30*time.Second); e != nil {
				slog.Error("claim", "error", e)
			}
			if e := s.Expire(ctx); e != nil {
				slog.Error("expire", "error", e)
			}
			if e := s.reminders(ctx); e != nil && ctx.Err() == nil {
				slog.Error("reminders", "error", e)
			}
			next = time.Now().Add(5 * time.Second)
		}
		if _, e := s.Read(ctx, name); e != nil && ctx.Err() == nil {
			slog.Error("consume", "error", e)
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
	owner, e := s.Redis.Get(ctx, fmt.Sprintf("%squeued:%d", s.Prefix, id)).Int64()
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

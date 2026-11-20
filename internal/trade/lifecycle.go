package trade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"xzdp/biz/model/message"
	"xzdp/biz/pkg/constants"
)

const unpaid = 1
const cancelled = 4

func (s Sale) Cancel(ctx context.Context, id string, user int64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v int64
	var status int
	if err = tx.QueryRowContext(ctx, "SELECT voucher_id,status FROM tb_voucher_order WHERE id=? AND user_id=? FOR UPDATE", id, user).Scan(&v, &status); err != nil {
		return err
	}
	if status == cancelled {
		return tx.Commit()
	}
	if status != unpaid {
		return errors.New("订单不可取消")
	}
	if _, err = tx.ExecContext(ctx, "UPDATE tb_voucher_order SET status=? WHERE id=?", cancelled, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=?", v); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO trade_release(order_id,voucher_id) VALUES(?,?)", id, v); err != nil {
		return err
	}
	return tx.Commit()
}

var joinWait = redis.NewScript(`
local a=redis.call('HMGET',KEYS[1],'stock','start','end')
local t=redis.call('TIME')
if not a[1] or tonumber(t[1])<tonumber(a[2]) or tonumber(t[1])>=tonumber(a[3]) then return {0,'活动未开放'} end
if tonumber(a[1])>0 then return {0,'仍有库存'} end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return {0,'已下单'} end
redis.call('ZADD',KEYS[3],'NX',tonumber(t[1])*1000000+tonumber(t[2]),ARGV[1]);return {1,'waiting'}`)

func (s Sale) Wait(ctx context.Context, v, u int64) (string, error) {
	if err := s.Warm(ctx, v); err != nil {
		return "", err
	}
	k := s.key(v)
	return checked(joinWait.Run(ctx, s.Redis, []string{k, k + ":buyers", k + ":wait"}, u))
}

var releaseStock = redis.NewScript(allocateOrder + `
if redis.call('SISMEMBER',KEYS[4],ARGV[1])==1 then return 0 end
if redis.call('EXISTS',KEYS[1])==0 then return redis.error_reply('activity not initialized') end
local now=tonumber(redis.call('TIME')[1])
local user=redis.call('ZRANGE',KEYS[3],0,0)[1]
if user and now<tonumber(redis.call('HGET',KEYS[1],'end')) then
 local id=allocate(KEYS[7])
 redis.call('ZREM',KEYS[3],user)
 redis.call('HSET',KEYS[2],user,id)
 redis.call('HSET',KEYS[5],id,ARGV[2]..':'..user)
 redis.call('ZADD',KEYS[6],now,id)
else redis.call('HINCRBY',KEYS[1],'stock',1) end
redis.call('SADD',KEYS[4],ARGV[1])
return 1`)

func (s Sale) release(ctx context.Context, id string, v int64) error {
	k := s.key(v)
	if err := s.Warm(ctx, v); err != nil {
		return err
	}
	err := releaseStock.Run(ctx, s.Redis, []string{k, k + ":buyers", k + ":wait", k + ":released", s.Prefix + "pending", s.Prefix + "relay", s.Prefix + "order-sequence"}, id, v).Err()
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "DELETE FROM trade_release WHERE order_id=?", id)
	return err
}

func (s Sale) Subscribe(ctx context.Context, v, u int64) error {
	var future bool
	if err := s.DB.QueryRowContext(ctx, "SELECT begin_time>NOW() FROM tb_seckill_voucher WHERE voucher_id=?", v).Scan(&future); err != nil {
		return err
	}
	if !future {
		return errors.New("活动已开始")
	}
	_, err := s.DB.ExecContext(ctx, "INSERT IGNORE INTO voucher_subscription(voucher_id,user_id) VALUES(?,?)", v, u)
	return err
}

type Notice struct {
	ID      int64  `json:"id"`
	Content string `json:"content"`
}

func (s Sale) Inbox(ctx context.Context, u int64) ([]Notice, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT id,content FROM trade_notice WHERE user_id=? ORDER BY id DESC LIMIT 50", u)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Notice, 0)
	for rows.Next() {
		var n Notice
		if err = rows.Scan(&n.ID, &n.Content); err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

func (s Sale) expire(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT id,user_id FROM tb_voucher_order WHERE status=1 AND create_time<=NOW()-INTERVAL 15 MINUTE ORDER BY create_time LIMIT 100")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var u int64
		if err = rows.Scan(&id, &u); err != nil {
			return err
		}
		if err = s.Cancel(ctx, id, u); err != nil {
			return err
		}
	}
	return rows.Err()
}
func (s Sale) releaseBatch(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT order_id,voucher_id FROM trade_release ORDER BY created_at LIMIT 100")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var v int64
		if err = rows.Scan(&id, &v); err != nil {
			return err
		}
		if err = s.release(ctx, id, v); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s Sale) reminders(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `INSERT IGNORE INTO trade_notice(event_key,user_id,content)
 SELECT CONCAT('opening:',v.voucher_id,':',s.user_id),s.user_id,CONCAT('优惠券 ',v.voucher_id,' 即将开抢')
 FROM voucher_subscription s JOIN tb_seckill_voucher v ON s.voucher_id=v.voucher_id
 WHERE v.begin_time BETWEEN NOW() AND NOW()+INTERVAL 5 MINUTE`)
	return err
}

var pushNotice = redis.NewScript(`if redis.call('SET',KEYS[2],1,'NX','EX',604800) then redis.call('XADD',KEYS[1],'*','message',ARGV[1]) end return 1`)

func (s Sale) dispatch(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT id,user_id,content FROM trade_notice WHERE pushed=0 ORDER BY id LIMIT 100")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, u int64
		var text string
		if err = rows.Scan(&id, &u, &text); err != nil {
			return err
		}
		body, _ := json.Marshal(message.Message{To: u, Content: text, Type: "voucher", Time: time.Now().Format(time.RFC3339)})
		if err = pushNotice.Run(ctx, s.Redis, []string{constants.MESSAGE_STREAM_KEY + strconv.FormatInt(u, 10), fmt.Sprintf("%snotice:%d", s.Prefix, id)}, body).Err(); err != nil {
			return err
		}
		if _, err = s.DB.ExecContext(ctx, "UPDATE trade_notice SET pushed=1 WHERE id=?", id); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s Sale) reconcile(ctx context.Context, cursor uint64) (uint64, error) {
	items, next, err := s.Redis.HScan(ctx, s.Prefix+"pending", cursor, "*", 100).Result()
	if err != nil {
		return cursor, err
	}
	for i := 0; i < len(items); i += 2 {
		id := items[i]
		o, err := s.pending(ctx, id)
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return cursor, err
		}
		var stock int
		var existing, dead bool
		err = s.DB.QueryRowContext(ctx, `SELECT stock,EXISTS(SELECT 1 FROM tb_voucher_order WHERE id=? AND voucher_id=? AND user_id=?),EXISTS(SELECT 1 FROM trade_dead_letter WHERE order_id=?) FROM tb_seckill_voucher WHERE voucher_id=?`, id, o.Voucher, o.User, id, o.Voucher).Scan(&stock, &existing, &dead)
		if err != nil {
			return cursor, err
		}
		if existing {
			if err = s.ack(ctx, id); err != nil {
				return cursor, err
			}
			continue
		}
		if dead && stock < 1 {
			slog.Warn("reservation needs review", "order", id, "dbStock", stock)
			continue
		}
		// NX preserves the relay backoff for records already in flight.
		added, err := redis.NewScript(`if redis.call('HEXISTS',KEYS[1],ARGV[1])==0 then return 0 end return redis.call('ZADD',KEYS[2],'NX',ARGV[2],ARGV[1])`).Run(ctx, s.Redis, []string{s.Prefix + "pending", s.Prefix + "relay"}, id, time.Now().Unix()).Int64()
		if err != nil {
			return cursor, err
		}
		if added > 0 {
			slog.Warn("reconciled missing delivery", "order", id, "deadLetter", dead, "dbStock", stock)
		}
	}
	return next, nil
}

func (s Sale) Maintain(ctx context.Context) error {
	var cursor uint64
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			for _, step := range []func(context.Context) error{s.expire, s.releaseBatch, s.reminders, s.dispatch} {
				if err := step(ctx); err != nil && ctx.Err() == nil {
					slog.Warn("voucher maintenance", "error", err)
				}
			}
			next, err := s.reconcile(ctx, cursor)
			if err != nil && ctx.Err() == nil {
				slog.Warn("voucher reconciliation", "error", err)
			} else {
				cursor = next
			}
		}
	}
}

// Preheat before traffic reaches the admission endpoint. Token also warms newly created sales.
func (s Sale) Preheat(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT voucher_id FROM tb_seckill_voucher WHERE begin_time<=NOW() AND end_time>NOW()")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err = rows.Scan(&v); err != nil {
			return err
		}
		if err = s.Warm(ctx, v); err != nil {
			return err
		}
	}
	return rows.Err()
}

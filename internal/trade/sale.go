// Package trade 负责优惠券秒杀受理、分桶库存管理、订单异步落库与核销。
package trade

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/metrics"
	"xzdp/internal/mq"
)

type Sale struct {
	Queue     *mq.Queue
	Publisher interface {
		Publish(context.Context, string, []byte) error
	}
	ReconcileAge time.Duration
	DB           *sql.DB
	Redis        *redis.Client
	Prefix       string
	Buckets      int
	Metrics      *metrics.Metrics
}

func (s Sale) key(v int64) string { return fmt.Sprintf("%sv:%d", s.Prefix, v) }

var warmScript = redis.NewScript(`if redis.call('EXISTS',KEYS[1])==0 then
 local n=tonumber(ARGV[4])
 redis.call('HSET',KEYS[1],'buckets',n,'start',ARGV[2],'end',ARGV[3])
 for i=0,n-1 do redis.call('SET',KEYS[1]..':stock:'..i, math.floor(ARGV[1]/n)+(i<tonumber(ARGV[1])%n and 1 or 0)) end
 for i=5,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end end return 1`)

func (s Sale) Warm(ctx context.Context, v int64) error {
	key := s.key(v)
	exists, err := s.Redis.Exists(ctx, key).Result()
	if err != nil || exists > 0 {
		return err
	}
	// Serialize cold initialization with stock changes in PostgreSQL.
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var stock, start, end, unreleased int64
	if err = tx.QueryRowContext(ctx, "SELECT stock,floor(extract(epoch FROM begin_time))::bigint,floor(extract(epoch FROM end_time))::bigint FROM tb_seckill_voucher WHERE voucher_id=$1 FOR UPDATE", v).Scan(&stock, &start, &end); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM trade_release WHERE voucher_id=$1", v).Scan(&unreleased); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT user_id,id FROM tb_voucher_order WHERE voucher_id=$1", v)
	if err != nil {
		return err
	}
	args := []interface{}{stock - unreleased, start, end, max(1, min(s.Buckets, 64))}
	for rows.Next() {
		var u, id string
		if err = rows.Scan(&u, &id); err != nil {
			rows.Close()
			return err
		}
		args = append(args, u, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	err = warmScript.Run(ctx, s.Redis, []string{key, key + ":buyers"}, args...).Err()
	if err != nil {
		return err
	}
	return tx.Commit()
}

const unpaid = 1

func (s Sale) Pending() string { return s.Prefix + "pending" }

const queueLua = `
local function enqueue(pending,queued,id,voucher,user,bucket)
 local now=redis.call('TIME')
 local payload=cjson.encode({id=id,voucher=voucher,user=user,bucket=tostring(bucket)})
 redis.call('HSET',queued,'user',user,'payload',payload)
 redis.call('ZADD',pending,tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000),id)
 return payload
end
`
const reserveLua = queueLua + `
local prior=redis.call('HGET',KEYS[2],ARGV[2])
if prior then return prior end
local t=tonumber(redis.call('TIME')[1])
local a=redis.call('HMGET',KEYS[1],'start','end','buckets')
if not a[1] then return 'cold' end
if t<tonumber(a[1]) or t>=tonumber(a[2]) then return 'closed' end
local n=tonumber(a[3])
for offset=0,n-1 do
 local b=(tonumber(ARGV[2])%n+offset)%n
 local stock=KEYS[1]..':stock:'..b
 if tonumber(redis.call('GET',stock))>0 then
  local payload=enqueue(KEYS[3],KEYS[4],ARGV[3],ARGV[1],ARGV[2],b)
  redis.call('DECR',stock)
  redis.call('ZREM',KEYS[5],ARGV[2])
  redis.call('HSET',KEYS[2],ARGV[2],ARGV[3])
  return payload
 end
end
return 'soldout'`

var reserveScript = redis.NewScript(reserveLua)

const SoldOut = api.Problem("已售罄")

func newOrderID() string {
	var nonce [8]byte
	rand.Read(nonce[:])
	return strconv.FormatUint((binary.BigEndian.Uint64(nonce[:])>>1)|1, 10)
}
func (s Sale) Reserve(ctx context.Context, v, u int64) (string, error) {
	id := newOrderID()
	k := s.key(v)
	run := func() (string, error) {
		return reserveScript.Run(ctx, s.Redis, []string{k, k + ":buyers", s.Pending(), s.Prefix + "queued:" + id, k + ":wait"}, v, u, id).Text()
	}
	result, e := run()
	if result == "cold" && e == nil {
		if e = s.Warm(ctx, v); e != nil {
			return "", e
		}
		result, e = run()
	}
	if e == nil && len(result) > 0 && result[0] == '{' {
		s.publishAdmission(ctx, id, result)
		return id, nil
	}
	switch result {
	case "soldout":
		return "", SoldOut
	case "closed", "cold":
		return "", api.Problem("活动未开放")
	}
	return result, e
}

func (s Sale) publishAdmission(ctx context.Context, id, payload string) {
	if s.Publisher == nil {
		return
	}
	if err := s.Publisher.Publish(ctx, id, []byte(payload)); err != nil {
		slog.Warn("order pending Kafka reconciliation", "order", id, "error", err)
		return
	}
	s.Metrics.MQ("produce", 1)
}

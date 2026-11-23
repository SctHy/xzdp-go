// Package trade 负责优惠券秒杀受理、分桶库存管理、订单异步落库与核销。
package trade

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/metrics"
)

type Sale struct {
	DB      *sql.DB
	Redis   *redis.Client
	Prefix  string
	Buckets int
	Metrics *metrics.Metrics
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
const Group = "orders"

func (s Sale) Stream() string { return s.Prefix + "orders" }

const reserveLua = `
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
  redis.call('XADD',KEYS[3],'*','id',ARGV[3],'voucher',ARGV[1],'user',ARGV[2],'bucket',b)
  redis.call('DECR',stock)
  redis.call('ZREM',KEYS[5],ARGV[2])
  redis.call('HSET',KEYS[2],ARGV[2],ARGV[3])
  redis.call('SET',KEYS[4],ARGV[2],'EX',86400)
  return ARGV[3]
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
		return reserveScript.Run(ctx, s.Redis, []string{k, k + ":buyers", s.Stream(), s.Prefix + "queued:" + id, k + ":wait"}, v, u, id).Text()
	}
	result, e := run()
	if result == "cold" && e == nil {
		if e = s.Warm(ctx, v); e != nil {
			return "", e
		}
		result, e = run()
	}
	switch result {
	case "soldout":
		return "", SoldOut
	case "closed", "cold":
		return "", api.Problem("活动未开放")
	}
	return result, e
}

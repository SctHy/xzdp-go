package trade

import (
	"context"

	"github.com/redis/go-redis/v9"

	api "xzdp/internal/httpapi/endpoint"
)

// A cancellation and its stock-return intent commit together. Redis release can be retried.
func (s Sale) Cancel(ctx context.Context, id, u int64) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var voucher int64
	var status int
	if e = tx.QueryRowContext(ctx, "SELECT voucher_id,status FROM tb_voucher_order WHERE id=$1 AND user_id=$2 FOR UPDATE", id, u).Scan(&voucher, &status); e != nil {
		return e
	}
	if status == 4 {
		return tx.Commit()
	}
	if status != unpaid {
		return api.Problem("只可取消未支付订单")
	}
	if _, e = tx.ExecContext(ctx, "UPDATE tb_voucher_order SET status=4 WHERE id=$1", id); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=$1", voucher); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO trade_release(order_id,voucher_id) VALUES($1,$2)", id, voucher); e != nil {
		return e
	}
	return tx.Commit()
}

var waitScript = redis.NewScript(`
local a=redis.call('HMGET',KEYS[1],'start','end','buckets')
local t=redis.call('TIME')
if tonumber(t[1])<tonumber(a[1]) or tonumber(t[1])>=tonumber(a[2]) then return 'closed' end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return 'bought' end
for i=0,tonumber(a[3])-1 do if tonumber(redis.call('GET',KEYS[1]..':stock:'..i))>0 then return 'available' end end
redis.call('ZADD',KEYS[3],'NX',tonumber(t[1])*1000000+tonumber(t[2]),ARGV[1])
return 'waiting'`)

func (s Sale) Wait(ctx context.Context, v, u int64) (string, error) {
	if e := s.Warm(ctx, v); e != nil {
		return "", e
	}
	key := s.key(v)
	result, e := waitScript.Run(ctx, s.Redis, []string{key, key + ":buyers", key + ":wait"}, u).Text()
	if e != nil {
		return "", e
	}
	switch result {
	case "available":
		return "", api.Problem("仍有库存，可直接抢购")
	case "bought":
		return "", api.Problem("已下单")
	case "closed":
		return "", api.Problem("活动未开放")
	}
	return result, nil
}

// Return to the exact bucket or atomically promote the oldest waiting buyer.
const releaseLua = queueLua + `
if redis.call('SISMEMBER',KEYS[2],ARGV[1])==1 then return '' end
local n=tonumber(redis.call('HGET',KEYS[1],'buckets'))
local bucket=tonumber(ARGV[2])%n
local buyer=redis.call('ZRANGE',KEYS[3],0,0)[1]
local result=''
if buyer and tonumber(redis.call('TIME')[1])<tonumber(redis.call('HGET',KEYS[1],'end')) then
 result=enqueue(KEYS[5],KEYS[6],ARGV[4],ARGV[3],buyer,bucket)
 redis.call('HSET',KEYS[4],buyer,ARGV[4])
 redis.call('ZREM',KEYS[3],buyer)
else
 redis.call('INCR',KEYS[1]..':stock:'..bucket)
end
redis.call('SADD',KEYS[2],ARGV[1])
return result`

var releaseScript = redis.NewScript(releaseLua)

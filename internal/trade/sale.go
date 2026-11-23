// Package trade owns voucher admission and order persistence.
package trade

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"

	"xzdp/internal/observe"
)

type Sale struct {
	DB     *sql.DB
	Redis  *redis.Client
	Prefix string
}
type Order struct {
	ID      string `json:"id"`
	Voucher int64  `json:"voucher"`
	User    int64  `json:"user"`
}

func (s Sale) key(v int64) string { return fmt.Sprintf("%sv:%d", s.Prefix, v) }
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
	args := []interface{}{stock - unreleased, start, end}
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
	err = redis.NewScript(`if redis.call('EXISTS',KEYS[1])==0 then
 redis.call('HSET',KEYS[1],'stock',ARGV[1],'start',ARGV[2],'end',ARGV[3])
 for i=4,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end end return 1`).Run(ctx, s.Redis, []string{key, key + ":buyers"}, args...).Err()
	if err != nil {
		return err
	}
	return tx.Commit()
}

var issue = redis.NewScript(`
local now=tonumber(redis.call('TIME')[1])
local a=redis.call('HMGET',KEYS[1],'start','end','stock')
if not a[1] or now<tonumber(a[1]) or now>=tonumber(a[2]) then return {0,'活动未开放'} end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return {0,'已下单'} end
if tonumber(a[3])<=0 then return {0,'已售罄'} end
local old=redis.call('GET',KEYS[3])
if old then return {1,old} end
redis.call('SET',KEYS[3],ARGV[2],'EX',30)
return {1,ARGV[2]}`)

func checked(cmd *redis.Cmd) (string, error) {
	v, err := cmd.Slice()
	if err != nil {
		return "", err
	}
	if v[0].(int64) == 0 {
		observe.LuaRejections.Inc()
		return "", errors.New(v[1].(string))
	}
	return v[1].(string), nil
}
func (s Sale) Token(ctx context.Context, v, u int64) (string, error) {
	if err := s.Warm(ctx, v); err != nil {
		return "", err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	k := s.key(v)
	return checked(issue.Run(ctx, s.Redis, []string{k, k + ":buyers", fmt.Sprintf("%s:token:%d", k, u)}, u, hex.EncodeToString(b[:])))
}

// GET returns the exact decimal ID even above Lua's integer precision.
const allocateOrder = `
local function allocate(key)
 redis.call('SET',key,'4611686018427387904','NX')
 redis.call('INCR',key)
 return redis.call('GET',key)
end
`

var reserve = redis.NewScript(allocateOrder + `
local now=tonumber(redis.call('TIME')[1])
local credential=redis.call('GET',KEYS[3])
if credential then
 local token,id=string.match(credential,'^(%x+):(%d+)$')
 if token then
  if token==ARGV[2] then return {1,id} end
  return {0,'令牌无效'}
 end
end
if credential~=ARGV[2] then return {0,'令牌无效'} end
local a=redis.call('HMGET',KEYS[1],'start','end','stock')
if not a[1] or now<tonumber(a[1]) or now>=tonumber(a[2]) then return {0,'活动未开放'} end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return {0,'已下单'} end
if tonumber(a[3])<=0 then return {0,'已售罄'} end
local id=allocate(KEYS[6])
redis.call('HINCRBY',KEYS[1],'stock',-1)
redis.call('HSET',KEYS[2],ARGV[1],id)
redis.call('SET',KEYS[3],ARGV[2]..':'..id,'EX',900)
redis.call('HSET',KEYS[4],id,ARGV[3])
redis.call('ZADD',KEYS[5],now,id)
return {1,id}`)

func (s Sale) Reserve(ctx context.Context, v, u int64, token string) (string, error) {
	if token == "" {
		return "", errors.New("缺少秒杀令牌")
	}
	k := s.key(v)
	return checked(reserve.Run(ctx, s.Redis, []string{k, k + ":buyers", fmt.Sprintf("%s:token:%d", k, u), s.Prefix + "pending", s.Prefix + "relay", s.Prefix + "order-sequence"}, u, token, fmt.Sprintf("%d:%d", v, u)))
}
func (s Sale) pending(ctx context.Context, id string) (Order, error) {
	data, err := s.Redis.HGet(ctx, s.Prefix+"pending", id).Result()
	if err != nil {
		return Order{}, err
	}
	fields := strings.SplitN(data, ":", 2)
	if len(fields) != 2 {
		return Order{}, errors.New("invalid pending order")
	}
	v, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return Order{}, err
	}
	u, err := strconv.ParseInt(fields[1], 10, 64)
	return Order{id, v, u}, err
}
func (s Sale) Status(ctx context.Context, id string, u int64) (string, error) {
	// Read pending before SQL: the worker commits the order before clearing pending.
	o, pendingErr := s.pending(ctx, id)
	var status sql.NullInt64
	var dead bool
	err := s.DB.QueryRowContext(ctx, `SELECT
		(SELECT status FROM tb_voucher_order WHERE id=$1 AND user_id=$2),
		EXISTS(SELECT 1 FROM trade_dead_letter WHERE order_id=$1 AND user_id=$2)`, id, u).Scan(&status, &dead)
	if err != nil {
		return "", err
	}
	if status.Valid {
		return map[int64]string{unpaid: "unpaid", paid: "paid", used: "used", cancelled: "cancelled", 5: "refunding", 6: "refunded"}[status.Int64], nil
	}
	if pendingErr != nil && !errors.Is(pendingErr, redis.Nil) {
		return "", pendingErr
	}
	if pendingErr == nil && o.User == u {
		return "queued", nil
	}
	if dead {
		return "failed", nil
	}
	return "", sql.ErrNoRows
}

var window = redis.NewScript(`
local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000)
for i,k in ipairs(KEYS) do
redis.call('ZREMRANGEBYSCORE',k,'-inf',now-1000)
if redis.call('ZCARD',k)>=tonumber(ARGV[i]) then return 0 end end
for _,k in ipairs(KEYS) do redis.call('ZADD',k,now,ARGV[4]);redis.call('PEXPIRE',k,1100) end return 1`)

func (s Sale) Allow(ctx context.Context, ip, path, request string, user int64) (bool, error) {
	prefix := s.Prefix + "limit:"
	n, err := window.Run(ctx, s.Redis, []string{prefix + "ip:" + ip, prefix + "route:" + path, prefix + "user:" + strconv.FormatInt(user, 10)}, 2500, 5000, 10, request).Int()
	if n == 0 && err == nil {
		observe.LuaRejections.Inc()
	}
	return n == 1, err
}

func Prefix(env string) string { return "xzdp:" + env + ":trade:" }

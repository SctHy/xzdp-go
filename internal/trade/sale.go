// Package trade owns voucher admission and order persistence.
package trade

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
	"sync/atomic"
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

var Rejections atomic.Uint64

func (s Sale) key(v int64) string { return fmt.Sprintf("%sv:%d", s.Prefix, v) }
func (s Sale) Warm(ctx context.Context, v int64) error {
	key := s.key(v)
	exists, err := s.Redis.Exists(ctx, key).Result()
	if err != nil || exists > 0 {
		return err
	}
	var stock, start, end int64
	if err = s.DB.QueryRowContext(ctx, "SELECT stock,UNIX_TIMESTAMP(begin_time),UNIX_TIMESTAMP(end_time) FROM tb_seckill_voucher WHERE voucher_id=?", v).Scan(&stock, &start, &end); err != nil {
		return err
	}
	return redis.NewScript(`if redis.call('EXISTS',KEYS[1])==0 then redis.call('HSET',KEYS[1],'stock',ARGV[1],'start',ARGV[2],'end',ARGV[3]) end return 1`).Run(ctx, s.Redis, []string{key}, stock, start, end).Err()
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
		Rejections.Add(1)
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

var reserve = redis.NewScript(`
local now=tonumber(redis.call('TIME')[1])
if redis.call('GET',KEYS[3])~=ARGV[2] then return {0,'令牌无效'} end
local a=redis.call('HMGET',KEYS[1],'start','end','stock')
if not a[1] or now<tonumber(a[1]) or now>=tonumber(a[2]) then return {0,'活动未开放'} end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return {0,'已下单'} end
if tonumber(a[3])<=0 then return {0,'已售罄'} end
redis.call('HINCRBY',KEYS[1],'stock',-1)
redis.call('HSET',KEYS[2],ARGV[1],ARGV[3])
redis.call('DEL',KEYS[3])
redis.call('HSET',KEYS[4],ARGV[3],ARGV[4])
redis.call('ZADD',KEYS[5],now,ARGV[3])
return {1,ARGV[3]}`)

func (s Sale) Reserve(ctx context.Context, v, u int64, token string) (string, error) {
	if token == "" {
		return "", errors.New("缺少秒杀令牌")
	}
	id, err := s.nextID(ctx)
	if err != nil {
		return "", err
	}
	k := s.key(v)
	return checked(reserve.Run(ctx, s.Redis, []string{k, k + ":buyers", fmt.Sprintf("%s:token:%d", k, u), s.Prefix + "pending", s.Prefix + "relay"}, u, token, id, fmt.Sprintf("%d:%d", v, u)))
}
func (s Sale) nextID(ctx context.Context) (string, error) {
	// Redis serializes the sequence across all API and worker instances.
	seq, err := s.Redis.Incr(ctx, s.Prefix+"sequence").Result()
	return strconv.FormatInt((1<<61)+seq, 10), err
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
	var status int
	err := s.DB.QueryRowContext(ctx, "SELECT status FROM tb_voucher_order WHERE id=? AND user_id=?", id, u).Scan(&status)
	if err == nil {
		return map[int]string{1: "unpaid", 2: "paid", 3: "used", 4: "cancelled", 5: "refunding", 6: "refunded"}[status], nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	var failed bool
	if err = s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM trade_dead_letter WHERE order_id=? AND user_id=?)", id, u).Scan(&failed); err != nil {
		return "", err
	}
	if failed {
		return "failed", nil
	}
	o, err := s.pending(ctx, id)
	if err == nil && o.User == u {
		return "queued", nil
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
	return n == 1, err
}

func Prefix(env string) string { return "xzdp:" + env + ":trade:" }

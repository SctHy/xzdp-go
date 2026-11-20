// Package social owns like aggregation and feed delivery. Both use the existing blog records.
package social

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"

	"xzdp/biz/model/message"
	"xzdp/biz/pkg/constants"
)

type Store struct {
	DB     *sql.DB
	Redis  *redis.Client
	Prefix string
}

func (s Store) countKey(id int64) string { return fmt.Sprintf("%slike:%d", s.Prefix, id) }

var toggle = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==0 then
 if #ARGV<7 then return -1 end
 redis.call('HSET',KEYS[1],'count',ARGV[5],'version',ARGV[6],'author',ARGV[7])
end
local author=redis.call('HGET',KEYS[1],'author')
local on=redis.call('ZREM',KEYS[2],ARGV[1])==0
local delta=-1
if on then
 redis.call('ZADD',KEYS[2],ARGV[2],ARGV[1])
 local notification=string.gsub(ARGV[3],'"to":0','"to":'..author,1)
 redis.call('XADD',ARGV[4]..author,'*','message',notification);delta=1
end
local n=redis.call('HINCRBY',KEYS[1],'count',delta)
if n<0 then redis.call('HSET',KEYS[1],'count',0) end
redis.call('HINCRBY',KEYS[1],'version',1)
redis.call('PERSIST',KEYS[1])
redis.call('SADD',KEYS[3],KEYS[1])
if on then return 1 else return 0 end`)

func (s Store) Toggle(ctx context.Context, id, u int64) (bool, error) {
	keys := []string{s.countKey(id), constants.BLOG_LIKED_KEY + strconv.FormatInt(id, 10), s.Prefix + "dirty"}
	now := time.Now()
	msg := message.Message{From: u, Content: "点赞了你的博客", Type: "like", Time: now.Format(time.RFC3339)}
	// The destination is read from the same cached snapshot as the count.
	body, _ := json.Marshal(msg)
	args := []interface{}{u, now.UnixMilli(), body, constants.MESSAGE_STREAM_KEY}
	n, err := toggle.Run(ctx, s.Redis, keys, args...).Int()
	if err != nil || n != -1 {
		return n == 1, err
	}
	var count, version, author int64
	if err = s.DB.QueryRowContext(ctx, "SELECT COALESCE(liked,0),like_version,user_id FROM tb_blog WHERE id=$1", id).Scan(&count, &version, &author); err != nil {
		return false, err
	}
	n, err = toggle.Run(ctx, s.Redis, keys, append(args, count, version, author)...).Int()
	return n == 1, err
}

var clean = redis.NewScript(`if redis.call('HGET',KEYS[1],'version')==ARGV[1] then redis.call('SREM',KEYS[2],KEYS[1]);redis.call('EXPIRE',KEYS[1],1800) end return 1`)

func (s Store) flush(ctx context.Context, key string) error {
	fields, err := s.Redis.HMGet(ctx, key, "count", "version").Result()
	if err != nil {
		return err
	}
	if fields[0] == nil {
		return nil
	}
	var id int64
	if _, err = fmt.Sscanf(key, s.Prefix+"like:%d", &id); err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "UPDATE tb_blog SET liked=$1,like_version=$2 WHERE id=$3 AND like_version<$4", fields[0], fields[1], id, fields[1])
	if err != nil {
		return err
	}
	return clean.Run(ctx, s.Redis, []string{key, s.Prefix + "dirty"}, fields[1]).Err()
}
func (s Store) Flush(ctx context.Context) error {
	keys, err := s.Redis.SRandMemberN(ctx, s.Prefix+"dirty", 128).Result()
	if err != nil {
		return err
	}
	var g errgroup.Group
	g.SetLimit(4)
	for _, key := range keys {
		key := key
		g.Go(func() error { return s.flush(ctx, key) })
	}
	return g.Wait()
}
func (s Store) Forget(ctx context.Context, id int64) error {
	key := s.countKey(id)
	_, err := s.Redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, key, constants.BLOG_LIKED_KEY+strconv.FormatInt(id, 10))
		p.SRem(ctx, s.Prefix+"dirty", key)
		return nil
	})
	return err
}

func (s Store) Count(ctx context.Context, id, fallback int64) int64 {
	n, err := s.Redis.HGet(ctx, s.countKey(id), "count").Int64()
	if err != nil {
		return fallback
	}
	return n
}
func (s Store) Run(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.Flush(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("flush blog likes", "error", err)
			}
			if err := s.deliver(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("deliver feed", "error", err)
			}
		}
	}
}

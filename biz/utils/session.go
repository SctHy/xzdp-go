package utils

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"

	"xzdp/biz/pkg/constants"
)

func CreateSession(ctx context.Context, r *redis.Client, id int64, nick, icon string) (string, error) {
	token, e := RandomUUID()
	if e != nil {
		return "", e
	}
	key := constants.LOGIN_USER_KEY + token
	_, e = r.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, "id", id, "nick_name", nick, "icon", icon)
		p.Expire(ctx, key, time.Minute)
		return nil
	})
	return token, e
}

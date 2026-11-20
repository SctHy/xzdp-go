package blog

import (
	"xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
	"xzdp/conf"
	"xzdp/internal/social"
)

func socialStore() (social.Store, error) {
	db, err := postgres.DB.DB()
	return social.Store{DB: db, Redis: redis.RedisClient, Prefix: "xzdp:" + conf.GetEnv() + ":social:"}, err
}

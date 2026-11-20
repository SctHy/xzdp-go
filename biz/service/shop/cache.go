package shop

import (
	"xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
	"xzdp/biz/pkg/cache"
	"xzdp/conf"
)

var shops *cache.ShopCache

// ConfigureCache is called once during startup, before registering HTTP routes.
func ConfigureCache(cfg *conf.Config) *cache.ShopCache {
	shops = cache.NewShopCache(cfg.Env, cfg.ShopCache, redis.RedisClient, cache.ShopSource{Detail: mysql.QueryByID, Types: mysql.QueryShopType, IDs: mysql.ShopIDs, Exists: mysql.ShopExists})
	return shops
}

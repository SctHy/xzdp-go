package dal

import (
	"context"
	"time"
	"xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
)

// Init initializes the dependencies for existing standalone callers.
func Init() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := mysql.Open(ctx)
	if err != nil {
		panic(err)
	}
	if err = redis.Open(ctx); err != nil {
		pool.Close()
		panic(err)
	}
}

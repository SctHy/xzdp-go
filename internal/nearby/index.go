// Package nearby maintains the Redis projection of shop coordinates.
package nearby

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
)

func Prefix(env string) string { return "xzdp:" + env + ":geo:" }

type Index struct {
	DB     *sql.DB
	Redis  *redis.Client
	Prefix string
}

var apply = redis.NewScript(`
local old=redis.call('HGET',KEYS[1],ARGV[1])
if old and old~=ARGV[2] then redis.call('ZREM',ARGV[5]..old,ARGV[1]) end
if ARGV[2]=='' then redis.call('HDEL',KEYS[1],ARGV[1])
else
 redis.call('GEOADD',ARGV[5]..ARGV[2],ARGV[3],ARGV[4],ARGV[1])
 redis.call('HSET',KEYS[1],ARGV[1],ARGV[2])
end
return 1`)

func (i Index) Sync(ctx context.Context) error {
	for ctx.Err() == nil {
		n, err := i.batch(ctx)
		if err != nil || n < 64 {
			return err
		}
	}
	return ctx.Err()
}

func (i Index) batch(ctx context.Context) (int, error) {
	tx, err := i.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT shop_id FROM shop_geo_dirty ORDER BY shop_id LIMIT 64 FOR UPDATE")
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, id := range ids {
		var category string
		var x, y float64
		err = tx.QueryRowContext(ctx, "SELECT type_id,x,y FROM tb_shop WHERE id=?", id).Scan(&category, &x, &y)
		if err != nil && err != sql.ErrNoRows {
			return 0, err
		}
		if err = apply.Run(ctx, i.Redis, []string{i.Prefix + "categories"}, id, category, x, y, i.Prefix).Err(); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM shop_geo_dirty WHERE shop_id=?", id); err != nil {
			return 0, err
		}
	}
	return len(ids), tx.Commit()
}
func (i Index) Warm(ctx context.Context) error {
	if _, err := i.DB.ExecContext(ctx, "INSERT IGNORE INTO shop_geo_dirty SELECT id FROM tb_shop"); err != nil {
		return err
	}
	if err := i.Sync(ctx); err != nil {
		return err
	}
	return i.Redis.Set(ctx, i.Prefix+"ready", 1, 0).Err()
}
func (i Index) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			present, err := i.Redis.Exists(ctx, i.Prefix+"ready").Result()
			if err == nil {
				if present == 0 {
					err = i.Warm(ctx)
				} else {
					err = i.Sync(ctx)
				}
			}
			if err != nil && ctx.Err() == nil {
				slog.Warn("refresh nearby shops", "error", err)
			}
		}
	}
}

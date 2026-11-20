package social

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
)

const rankCapacity = 100

// RefreshRank publishes the full list with one SET. WATCH prevents a slow
// builder from overwriting a newer snapshot published by another instance.
func (s Store) RefreshRank(ctx context.Context) error {
	key := s.Prefix + "hot-rank"
	err := s.Redis.Watch(ctx, func(tx *redis.Tx) error {
		ttl, err := tx.TTL(ctx, key).Result()
		if err != nil || ttl > 60*time.Second {
			return err
		}
		rows, err := s.DB.QueryContext(ctx, "SELECT b.id FROM tb_blog b JOIN tb_user u ON u.id=b.user_id ORDER BY b.liked DESC NULLS LAST,b.id DESC LIMIT $1", rankCapacity)
		if err != nil {
			return err
		}
		defer rows.Close()
		ids := make([]int64, 0, rankCapacity)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		data, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, key, data, 90*time.Second); return nil })
		return err
	}, key)
	if errors.Is(err, redis.TxFailedErr) {
		return nil
	}
	return err
}

func (s Store) RunRank(ctx context.Context) error {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		work, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := s.RefreshRank(work)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("refresh hot blogs", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// A page outside the bounded list is served by the existing SQL path.
func (s Store) RankPage(ctx context.Context, page, size int) ([]int64, error) {
	page = max(1, page)
	if size < 1 || page > rankCapacity/size {
		return nil, sql.ErrNoRows
	}
	data, err := s.Redis.Get(ctx, s.Prefix+"hot-rank").Bytes()
	if err != nil {
		return nil, err
	}
	var ids []int64
	if err = json.Unmarshal(data, &ids); err != nil {
		return nil, err
	}
	offset := min(len(ids), (page-1)*size)
	return ids[offset:min(offset+size, len(ids))], nil
}

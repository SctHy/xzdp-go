package store

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	Pool   *pgxpool.Pool
	DB     *sql.DB
	Redis  *redis.Client
	Prefix string
}

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func Open(ctx context.Context) (*Store, error) {
	pool, e := pgxpool.New(ctx, Env("DATABASE_URL", "postgres://xzdp:xzdp@localhost:5432/xzdp?sslmode=disable"))
	if e != nil {
		return nil, e
	}
	opts, e := redis.ParseURL(Env("REDIS_URL", "redis://localhost:6379/0"))
	if e != nil {
		pool.Close()
		return nil, e
	}
	s := &Store{pool, stdlib.OpenDBFromPool(pool), redis.NewClient(opts), Env("REDIS_PREFIX", "xzdp:")}
	if e = s.Ready(ctx); e == nil {
		e = Install(ctx, s.DB, Env("DEMO_DATA", "true") == "true")
	}
	if e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if e := s.Pool.Ping(ctx); e != nil {
		return e
	}
	return s.Redis.Ping(ctx).Err()
}
func (s *Store) Close() { s.Redis.Close(); s.DB.Close(); s.Pool.Close() }

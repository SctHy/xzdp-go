package trade

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestReleasePromotesOldestOnce(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	now := time.Now()
	mr.SetTime(now)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	s := Sale{Redis: r, Prefix: Prefix("test")}
	k := s.key(1)
	require.NoError(t, r.HSet(ctx, k, "stock", 0, "start", 0, "end", now.Add(time.Hour).Unix()).Err())
	_, err := s.Wait(ctx, 1, 20)
	require.NoError(t, err)
	mr.SetTime(now.Add(time.Second))
	_, err = s.Wait(ctx, 1, 10)
	require.NoError(t, err)
	keys := []string{k, k + ":buyers", k + ":wait", k + ":released", s.Prefix + "pending", s.Prefix + "relay", s.Prefix + "order-sequence"}
	released, err := releaseStock.Run(ctx, r, keys, "old", 1).Int()
	require.NoError(t, err)
	require.Equal(t, 1, released)
	repeated, err := releaseStock.Run(ctx, r, keys, "old", 1).Int()
	require.NoError(t, err)
	require.Zero(t, repeated)
	require.Equal(t, []string{"10"}, r.ZRange(ctx, k+":wait", 0, -1).Val())
	id := r.HGet(ctx, k+":buyers", "20").Val()
	require.Equal(t, "4611686018427387905", id)
	require.Equal(t, "1:20", r.HGet(ctx, s.Prefix+"pending", id).Val())
	require.Equal(t, "0", r.HGet(ctx, k, "stock").Val())
}

func TestDeliveryAfterCommitSkipsOrderAndStock(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	m.ExpectBegin()
	m.ExpectExec("INSERT IGNORE INTO trade_receipt").WithArgs("42").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectCommit()
	s := Sale{DB: db}
	require.NoError(t, s.persist(context.Background(), Order{ID: "42", User: 1, Voucher: 2}))
	require.NoError(t, m.ExpectationsWereMet())
}

package trade

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestReserveRetryReturnsOriginalOrder(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.SetTime(time.Now())
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	s := Sale{Redis: r, Prefix: Prefix("test")}
	ctx := context.Background()
	require.NoError(t, r.HSet(ctx, s.key(5), "start", 0, "end", time.Now().Add(time.Hour).Unix(), "stock", 2).Err())
	tok, err := s.Token(ctx, 5, 1)
	require.NoError(t, err)
	_, err = s.Reserve(ctx, 5, 2, tok)
	require.Error(t, err)
	id, err := s.Reserve(ctx, 5, 1, tok)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Reserve(ctx, 5, 1, tok)
			if err != nil || got != id {
				t.Errorf("retry = %q, %v; want %q", got, err, id)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, r.HLen(ctx, s.Prefix+"pending").Val())
	require.EqualValues(t, 1, r.ZCard(ctx, s.Prefix+"relay").Val())
	_, err = s.Reserve(ctx, 5, 1, "wrong")
	require.Error(t, err)
	mr.FastForward(15 * time.Minute)
	_, err = s.Reserve(ctx, 5, 1, tok)
	require.Error(t, err)
	_, err = s.Token(ctx, 5, 1)
	require.Error(t, err)
	o, err := s.pending(ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, o.User)
	require.Equal(t, "1", mr.HGet(s.key(5), "stock"))
}

func TestRateLimitHasIndependentWindows(t *testing.T) {
	mr := miniredis.RunT(t)
	now := time.Now()
	mr.SetTime(now)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	s := Sale{Redis: r, Prefix: Prefix("test")}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		ok, err := s.Allow(ctx, "1.2.3.4", "/token", strconv.Itoa(i), 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	ok, err := s.Allow(ctx, "1.2.3.5", "/reserve", "limit", 1)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = s.Allow(ctx, "1.2.3.4", "/token", "other", 2)
	require.NoError(t, err)
	require.True(t, ok)
	mr.SetTime(now.Add(2 * time.Second))
	ok, err = s.Allow(ctx, "1.2.3.4", "/token", "later", 1)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestStatusDuringDeliveryRecovery(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := Sale{DB: db, Redis: r, Prefix: Prefix("test")}
	require.NoError(t, r.HSet(ctx, s.Prefix+"pending", "42", "5:1").Err())
	check := func(user int64, status interface{}, dead bool, want string, wantErr error) {
		t.Helper()
		mock.ExpectQuery("SELECT .*SELECT status").WithArgs("42", user).WillReturnRows(sqlmock.NewRows([]string{"status", "dead"}).AddRow(status, dead))
		got, err := s.Status(ctx, "42", user)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, want, got)
	}
	check(1, nil, true, "queued", nil)
	check(2, nil, false, "", sql.ErrNoRows)
	check(1, unpaid, true, "unpaid", nil)
	require.NoError(t, s.ack(ctx, "42"))
	check(1, unpaid, true, "unpaid", nil)
	check(1, nil, true, "failed", nil)
	check(1, nil, false, "", sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

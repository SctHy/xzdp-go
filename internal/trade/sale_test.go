package trade

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestTokenBoundToBuyerAndConsumedOnce(t *testing.T) {
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

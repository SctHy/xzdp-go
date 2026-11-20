package verification

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestCodeAttemptsAndIPBudget(t *testing.T) {
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer r.Close()
	for i := 0; i < 21; i++ {
		k := fmt.Sprint(i)
		n, e := issue.Run(ctx, r, []string{"login:" + k, "cool:" + k, "ip"}, "012345").Int()
		require.NoError(t, e)
		if i < 20 {
			require.Equal(t, 1, n)
		} else {
			require.Zero(t, n)
		}
	}
	for i := 0; i < 5; i++ {
		n, e := verify.Run(ctx, r, []string{"login:0"}, "wrong").Int()
		require.NoError(t, e)
		require.Zero(t, n)
	}
	n, e := verify.Run(ctx, r, []string{"login:0"}, "012345").Int()
	require.NoError(t, e)
	require.Zero(t, n)
	n, e = verify.Run(ctx, r, []string{"login:1"}, "012345").Int()
	require.NoError(t, e)
	require.Equal(t, 1, n)
	n, e = verify.Run(ctx, r, []string{"login:1"}, "012345").Int()
	require.NoError(t, e)
	require.Zero(t, n)
}

// Package experiment exercises bucket-local admission in an isolated Redis Cluster.
package experiment

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

var admit = redis.NewScript(`
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 then return -1 end
if tonumber(redis.call('GET',KEYS[1]))==0 then return 0 end
redis.call('DECR',KEYS[1]);redis.call('HSET',KEYS[2],ARGV[1],'active');return 1`)
var release = redis.NewScript(`
if redis.call('HGET',KEYS[2],ARGV[1])~='active' then return 0 end
redis.call('HSET',KEYS[2],ARGV[1],'cancelled')
local waiter=redis.call('LPOP',KEYS[3])
if waiter then redis.call('HSET',KEYS[2],waiter,'active') else redis.call('INCR',KEYS[1]) end
return 1`)

// go-redis v8 expects older CLUSTER SLOTS replies. Read the primary endpoints
// from the Redis 7 response, which can also include endpoint metadata.
func slots(ctx context.Context, seed *redis.Client) ([]redis.ClusterSlot, error) {
	raw, e := seed.Do(ctx, "CLUSTER", "SLOTS").Slice()
	if e != nil {
		return nil, e
	}
	result := make([]redis.ClusterSlot, 0, len(raw))
	for _, v := range raw {
		fields := v.([]interface{})
		node := fields[2].([]interface{})
		result = append(result, redis.ClusterSlot{Start: int(fields[0].(int64)), End: int(fields[1].(int64)), Nodes: []redis.ClusterNode{{Addr: net.JoinHostPort(node[0].(string), fmt.Sprint(node[1]))}}})
	}
	return result, nil
}
func TestBucketExperiment(t *testing.T) {
	addr := os.Getenv("XZDP_EXPERIMENT_CLUSTER")
	if addr == "" {
		t.Skip("use script/load/buckets.sh")
	}
	ctx := context.Background()
	seed := redis.NewClient(&redis.Options{Addr: addr})
	defer seed.Close()
	cluster := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{addr}, PoolSize: 32, PoolTimeout: 15 * time.Second, ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) { return slots(ctx, seed) }})
	defer cluster.Close()
	for _, skew := range []bool{false, true} {
		for _, k := range []int{1, 4} {
			t.Run(fmt.Sprintf("K%d/skew_%t", k, skew), func(t *testing.T) {
				const clients = 1000
				keys := make([][]string, k)
				activity := time.Now().UnixNano()
				for b := range keys {
					tag := fmt.Sprintf("experiment:{%d:%d}", activity, b)
					keys[b] = []string{tag + ":stock", tag + ":buyers", tag + ":wait"}
					require.NoError(t, cluster.Set(ctx, keys[b][0], 100/k, 0).Err())
				}
				type answer struct {
					user, bucket, n int
					latency         time.Duration
					err             error
				}
				answers := make(chan answer, clients)
				var ready sync.WaitGroup
				ready.Add(clients)
				start := make(chan struct{})
				for j := 1; j <= clients; j++ {
					user := j
					if skew {
						user *= 4
					}
					go func(u int) {
						ready.Done()
						<-start
						b := u % k
						begin := time.Now()
						n, e := admit.Run(ctx, cluster, keys[b][:2], u).Int()
						answers <- answer{u, b, n, time.Since(begin), e}
					}(user)
				}
				ready.Wait()
				close(start)
				accepted := make([][]int, k)
				miss := make([][]int, k)
				lat := make([]time.Duration, 0, clients)
				ledger := map[int]bool{}
				for j := 0; j < clients; j++ {
					r := <-answers
					require.NoError(t, r.err)
					lat = append(lat, r.latency)
					if r.n == 1 {
						accepted[r.bucket] = append(accepted[r.bucket], r.user)
						ledger[r.user] = true
					} else {
						require.Zero(t, r.n)
						miss[r.bucket] = append(miss[r.bucket], r.user)
					}
				}
				expected := 100
				if skew {
					expected = 100 / k
				}
				require.Len(t, ledger, expected)
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				for b, ids := range accepted {
					if len(ids) == 0 {
						continue
					}
					keys := keys[b]
					u := ids[0]
					waiter := miss[b][0]
					n, e := admit.Run(ctx, cluster, keys[:2], u).Int()
					require.NoError(t, e)
					require.Equal(t, -1, n)
					require.NoError(t, cluster.RPush(ctx, keys[2], waiter).Err())
					n, e = release.Run(ctx, cluster, keys, u).Int()
					require.NoError(t, e)
					require.Equal(t, 1, n)
					delete(ledger, u)
					ledger[waiter] = true
					n, e = release.Run(ctx, cluster, keys, u).Int()
					require.NoError(t, e)
					require.Zero(t, n)
					n, e = release.Run(ctx, cluster, keys, waiter).Int()
					require.NoError(t, e)
					require.Equal(t, 1, n)
					delete(ledger, waiter)
				}
				free := 0
				actual := map[int]bool{}
				for _, key := range keys {
					n, e := cluster.Get(ctx, key[0]).Int()
					require.NoError(t, e)
					free += n
					buyers, e := cluster.HGetAll(ctx, key[1]).Result()
					require.NoError(t, e)
					for user, state := range buyers {
						if state == "active" {
							id, e := strconv.Atoi(user)
							require.NoError(t, e)
							actual[id] = true
						}
					}
				}
				require.Equal(t, ledger, actual)
				require.Equal(t, 100, free+len(actual))
				t.Logf("1000 requests, accepted=%d, stranded at admission=%d, admission P99=%s", expected, 100-expected, lat[989])
			})
		}
	}
}

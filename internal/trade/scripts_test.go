package trade

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAdmissionAndRelease(t *testing.T) {
	ctx := t.Context()
	r := miniredis.RunT(t)
	r.SetTime(time.Unix(100, 0))
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	defer client.Close()
	s := Sale{Redis: client, Prefix: "test:", Buckets: 2}
	k := s.key(1)
	warm := func() {
		t.Helper()
		if err := warmScript.Run(ctx, client, []string{k, k + ":buyers"}, 1, 90, 110, 2).Err(); err != nil {
			t.Fatal(err)
		}
	}
	warm()
	if _, err := s.Wait(ctx, 1, 1); err == nil {
		t.Fatal("wait accepted while stock available")
	}
	id, err := s.Reserve(ctx, 1, 1)
	if err != nil || id == "" {
		t.Fatal(id, err)
	}
	warm() // Replaying initialization must preserve the consumed stock.
	if replay, err := s.Reserve(ctx, 1, 1); err != nil || replay != id {
		t.Fatal(replay, err)
	}
	if _, err := s.Wait(ctx, 1, 1); err == nil {
		t.Fatal("buyer joined waitlist")
	}
	if _, err := s.Reserve(ctx, 1, 2); err != SoldOut {
		t.Fatal(err)
	}
	for i, buyer := range []int64{2, 3, 2} {
		if got, err := s.Wait(ctx, 1, buyer); err != nil || got != "waiting" {
			t.Fatal(got, err)
		}
		r.SetTime(time.Unix(101+int64(i), 0))
	}
	release := func(order, newID string, want int) {
		t.Helper()
		got, err := releaseScript.Run(ctx, client, []string{k, k + ":released", k + ":wait", k + ":buyers", s.Stream(), "test:queued:" + newID}, order, 0, 1, newID).Int()
		if err != nil || got != want {
			t.Fatal(got, err)
		}
	}
	release(id, "99", 1)
	release(id, "100", 0)
	owner, err := client.Get(ctx, "test:queued:99").Result()
	if err != nil || owner != "2" {
		t.Fatal(owner, err)
	}
	if n, err := client.XLen(ctx, s.Stream()).Result(); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	r.SetTime(time.Unix(110, 0))
	if _, err := s.Wait(ctx, 1, 4); err == nil {
		t.Fatal("closed sale allowed waiting")
	}
	if _, err := s.Reserve(ctx, 1, 4); err == nil {
		t.Fatal("closed sale allowed purchase")
	}
	release("99", "101", 1)
	release("99", "102", 0)
	if n, err := client.Get(ctx, k+":stock:0").Int(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

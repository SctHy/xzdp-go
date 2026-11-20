package health

import (
	"context"
	"errors"
	"github.com/cloudwego/hertz/pkg/app"
	"testing"
	"time"
)

func TestReadiness(t *testing.T) {
	ok := func(context.Context) error { return nil }
	fail := func(context.Context) error { return errors.New("offline") }
	h := Checks{MySQL: ok, Redis: ok}
	for _, tc := range []struct {
		check func(context.Context) error
		drain bool
		want  int
	}{{ok, false, 200}, {fail, false, 503}, {ok, true, 503}} {
		h.Redis = tc.check
		h.Draining.Store(tc.drain)
		c := app.NewContext(0)
		h.Ready(context.Background(), c)
		if c.Response.StatusCode() != tc.want {
			t.Fatalf("got %d want %d", c.Response.StatusCode(), tc.want)
		}
		Live(context.Background(), c)
		if c.Response.StatusCode() != 200 {
			t.Fatal("liveness depends on readiness")
		}
	}
}

func TestProbesShareDeadlineAndRunConcurrently(t *testing.T) {
	entered := make(chan time.Time, 2)
	release := make(chan struct{})
	probe := func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			return errors.New("missing deadline")
		}
		entered <- deadline
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h := Checks{MySQL: probe, Redis: probe}
	done := make(chan int, 1)
	go func() { c := app.NewContext(0); h.Ready(context.Background(), c); done <- c.Response.StatusCode() }()

	var deadline time.Time
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case d := <-entered:
			if i == 0 {
				deadline = d
			} else if !deadline.Equal(d) {
				t.Fatal("probes received different deadlines")
			}
		case <-timeout:
			t.Fatal("both probes must start")
		}
	}
	close(release)
	if code := <-done; code != 200 {
		t.Fatalf("readiness status %d", code)
	}
}

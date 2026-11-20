package health

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"golang.org/x/sync/errgroup"
	"sync/atomic"
	"time"
)

type Checks struct {
	Postgres, Redis func(context.Context) error
	Draining        atomic.Bool
}

func Live(_ context.Context, c *app.RequestContext) { c.JSON(200, map[string]string{"status": "ok"}) }

func (h *Checks) Ready(ctx context.Context, c *app.RequestContext) {
	if h.Draining.Load() {
		c.JSON(503, map[string]string{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var group errgroup.Group
	var dbOK, redisOK bool
	group.Go(func() error { dbOK = h.Postgres(ctx) == nil; return nil })
	group.Go(func() error { redisOK = h.Redis(ctx) == nil; return nil })
	group.Wait()
	code, status := 200, "ready"
	if !dbOK || !redisOK || h.Draining.Load() {
		code, status = 503, "unavailable"
	}
	c.JSON(code, map[string]interface{}{"status": status, "postgres": dbOK, "redis": redisOK})
}

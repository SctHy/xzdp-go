package interceptor

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/go-redis/redis/v8"

	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"
)

// Visitors batches the existing approximate UV statistic outside the request path.
type Visitors struct {
	redis  *redis.Client
	events chan string
}

func NewVisitors(r *redis.Client) *Visitors { return &Visitors{r, make(chan string, 2048)} }

func (v *Visitors) Record(ctx context.Context, c *app.RequestContext) {
	if user := utils.GetUser(ctx); user != nil {
		member := strconv.FormatInt(user.ID, 10) + time.Now().Format("2006-01-02")
		select {
		case v.events <- member:
		case <-ctx.Done():
		}
	}
	c.Next(ctx)
}

func (v *Visitors) Run(ctx context.Context) error {
	var batch []interface{}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		work, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := v.redis.PFAdd(work, constants.HLL_UV_KEY, batch...).Err(); err != nil {
			slog.Warn("write visitor count", "error", err)
		}
		batch = batch[:0]
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case member := <-v.events:
			batch = append(batch, member)
			if len(batch) == 256 {
				flush()
			}
		case <-tick.C:
			flush()
		case <-ctx.Done():
			for len(v.events) > 0 {
				batch = append(batch, <-v.events)
			}
			flush()
			return ctx.Err()
		}
	}
}

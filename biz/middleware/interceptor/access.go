package interceptor

import (
	"context"
	"errors"
	"github.com/cloudwego/hertz/pkg/app"
	"log/slog"
	"time"
	"xzdp/biz/utils"
)

func Access(ctx context.Context, c *app.RequestContext) {
	id, err := utils.RandomUUID()
	if err != nil {
		utils.SendErrResponse(ctx, c, 500, errors.New("internal error"))
		c.Abort()
		return
	}
	c.Header("X-Request-ID", id)
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			utils.SendErrResponse(ctx, c, 500, errors.New("internal error"))
			c.Abort()
			slog.ErrorContext(ctx, "request panic", "route", c.FullPath(), "requestId", id)
		}
		slog.InfoContext(ctx, "http request", "route", c.FullPath(), "method", string(c.Method()), "status", c.Response.StatusCode(), "code", c.GetString("errorCode"), "requestId", id, "duration", time.Since(start))
	}()
	c.Next(ctx)
}

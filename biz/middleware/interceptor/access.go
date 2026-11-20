package interceptor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/utils"
	"xzdp/conf"
	"xzdp/internal/observe"
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
		elapsed := time.Since(start)
		observe.HTTP(c.FullPath(), c.Response.StatusCode(), elapsed)
		if conf.GetConf().Hertz.EnableAccessLog || c.Response.StatusCode() >= 400 || c.GetString("errorCode") != "" {
			slog.InfoContext(ctx, "http request", "route", c.FullPath(), "method", string(c.Method()), "status", c.Response.StatusCode(), "code", c.GetString("errorCode"), "requestId", id, "duration", elapsed)
		}
	}()
	c.Next(ctx)
}

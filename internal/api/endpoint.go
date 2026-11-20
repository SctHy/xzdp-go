// Package api adapts business results to the existing Hertz response envelope.
package api

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/utils"
)

type Problem string

func (p Problem) Error() string { return string(p) }
func Endpoint(fn func(context.Context, *app.RequestContext) (any, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		value, err := fn(ctx, c)
		if err == nil {
			utils.SendSuccessResponse(ctx, c, 200, value)
			return
		}
		var problem Problem
		switch {
		case errors.As(err, &problem):
			utils.SendErrResponse(ctx, c, 200, problem)
		case errors.Is(err, sql.ErrNoRows):
			utils.SendErrResponse(ctx, c, 200, Problem("记录不存在"))
		default:
			slog.ErrorContext(ctx, "request failed", "route", c.FullPath(), "error", err)
			utils.SendErrResponse(ctx, c, 500, Problem("服务暂不可用"))
		}
	}
}
func ID(c *app.RequestContext) (int64, error) {
	n, e := strconv.ParseInt(c.Param("id"), 10, 64)
	if e != nil || n < 1 {
		return 0, Problem("无效编号")
	}
	return n, nil
}

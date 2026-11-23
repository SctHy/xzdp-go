// Package api adapts business results to the existing Hertz response envelope.
package api

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
)

type Problem string

func (p Problem) Error() string { return string(p) }
func Endpoint(fn func(context.Context, *app.RequestContext) (any, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		value, err := fn(ctx, c)
		if err == nil {
			c.JSON(200, map[string]any{"success": true, "data": value})
			return
		}
		var problem Problem
		switch {
		case errors.As(err, &problem):
			Fail(c, 400, problem.Error())
		case errors.Is(err, sql.ErrNoRows):
			Fail(c, 404, "记录不存在")
		default:
			slog.ErrorContext(ctx, "request failed", "route", c.FullPath(), "error", err)
			Fail(c, 503, "服务暂不可用")
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

type identity struct{}

func WithUser(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, identity{}, id)
}
func UserID(ctx context.Context) int64 { id, _ := ctx.Value(identity{}).(int64); return id }
func Fail(c *app.RequestContext, status int, message string) {
	c.JSON(status, map[string]any{"success": false, "errorMsg": message})
}

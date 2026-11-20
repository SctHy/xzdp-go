package trade

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"xzdp/biz/utils"
)

func (s Sale) Register(h *server.Hertz) {
	h.GET("/notifications", func(ctx context.Context, c *app.RequestContext) {
		items, err := s.Inbox(ctx, utils.GetUser(ctx).ID)
		if err != nil {
			utils.SendErrResponse(ctx, c, 500, err)
			return
		}
		utils.SendSuccessResponse(ctx, c, 200, items)
	})
	h.POST("/voucher-order/wait/:id", s.action(func(ctx context.Context, v, u int64, _ *app.RequestContext) (any, error) { return s.Wait(ctx, v, u) }))
	h.POST("/voucher-order/subscribe/:id", s.action(func(ctx context.Context, v, u int64, _ *app.RequestContext) (any, error) {
		return "subscribed", s.Subscribe(ctx, v, u)
	}))
	h.POST("/voucher-order/cancel/:id", s.action(func(ctx context.Context, id, u int64, _ *app.RequestContext) (any, error) {
		return "cancelled", s.Cancel(ctx, strconv.FormatInt(id, 10), u)
	}))
	h.POST("/voucher-order/token/:id", s.action(func(ctx context.Context, id, user int64, c *app.RequestContext) (any, error) {
		return s.Token(ctx, id, user)
	}))
	h.GET("/voucher-order/status/:id", s.action(func(ctx context.Context, id, user int64, c *app.RequestContext) (any, error) {
		return s.Status(ctx, strconv.FormatInt(id, 10), user)
	}))
}
func (s Sale) action(f func(context.Context, int64, int64, *app.RequestContext) (any, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		user := utils.GetUser(ctx)
		if user == nil {
			utils.SendErrResponse(ctx, c, 401, errors.New("请先登录"))
			return
		}
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id <= 0 {
			utils.SendErrResponse(ctx, c, 400, errors.New("invalid id"))
			return
		}
		data, err := f(ctx, id, user.ID, c)
		if err != nil {
			utils.SendErrResponse(ctx, c, 200, err)
			return
		}
		utils.SendSuccessResponse(ctx, c, 200, data)
	}
}
func (s Sale) Limit(ctx context.Context, c *app.RequestContext) {
	switch c.FullPath() {
	case "/voucher-order/token/:id", "/voucher-order/seckill/:id":
	default:
		c.Next(ctx)
		return
	}
	user := utils.GetUser(ctx)
	if user == nil {
		utils.SendErrResponse(ctx, c, 401, errors.New("请先登录"))
		c.Abort()
		return
	}
	ip := c.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	ok, err := s.Allow(ctx, ip, c.FullPath(), string(c.Response.Header.Peek("X-Request-ID")), user.ID)
	if err != nil || !ok {
		code := 429
		if err != nil {
			code = 503
		}
		utils.SendErrResponse(ctx, c, code, errors.New("暂时无法参与，请稍后重试"))
		c.Abort()
		return
	}
	c.Next(ctx)
}

package trade

import (
	"context"
	"database/sql"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/store"
)

func (s Sale) Register(h *server.Hertz, mock bool) {
	s.notifications(h)
	h.GET("/vouchers", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		return store.List(ctx, s.DB, "SELECT v.id,title,type,pay_value,actual_value,stock FROM tb_voucher v LEFT JOIN tb_seckill_voucher s ON s.voucher_id=v.id WHERE status=1 ORDER BY v.id")
	}))
	action := func(f func(context.Context, int64, int64, *app.RequestContext) (any, error)) app.HandlerFunc {
		return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
			id, e := api.ID(c)
			if e != nil {
				return nil, e
			}
			return f(ctx, id, api.UserID(ctx), c)
		})
	}
	h.POST("/vouchers/:id/buy", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) {
		order, e := s.Reserve(ctx, id, u)
		if !errors.Is(e, sql.ErrNoRows) {
			return order, e
		}
		var ordinary int64
		e = s.DB.QueryRowContext(ctx, `INSERT INTO ordinary_grant(user_id,voucher_id) SELECT $1,id FROM tb_voucher WHERE id=$2 AND status=1 AND type=0 ON CONFLICT(user_id,voucher_id) DO UPDATE SET voucher_id=excluded.voucher_id RETURNING voucher_id`, u, id).Scan(&ordinary)
		return "claimed", e
	}))
	h.POST("/orders/:id/cancel", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) {
		return "cancelled", s.Cancel(ctx, id, u)
	}))
	h.POST("/vouchers/:id/wait", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) { return s.Wait(ctx, id, u) }))
	h.GET("/wallet", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		return store.List(ctx, s.DB, `SELECT v.title,'领取' source FROM ordinary_grant g JOIN tb_voucher v ON v.id=g.voucher_id WHERE g.user_id=$1 UNION ALL SELECT v.title,to_char(g.month_start,'YYYY-MM')||' 满勤' FROM sign_reward_grant g JOIN tb_voucher v ON v.id=g.voucher_id WHERE g.user_id=$1 ORDER BY 1,2`, api.UserID(ctx))
	}))
	h.GET("/orders", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		return store.List(ctx, s.DB, "SELECT o.id::text id,v.title,o.status FROM tb_voucher_order o JOIN tb_voucher v ON v.id=o.voucher_id WHERE user_id=$1 ORDER BY o.create_time DESC LIMIT 100", api.UserID(ctx))
	}))
	h.GET("/orders/:id", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) {
		return s.Status(ctx, id, u)
	}))
	if mock {
		h.POST("/orders/:id/pay", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) { return s.Pay(ctx, id, u) }))
	}
	h.POST("/orders/:id/redeem", action(func(ctx context.Context, id, u int64, c *app.RequestContext) (any, error) {
		var v struct{ Code string }
		if c.BindJSON(&v) != nil {
			return nil, api.Problem("核销码无效")
		}
		return "used", s.Redeem(ctx, id, u, v.Code)
	}))
}

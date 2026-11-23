package trade

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/store"
)

func (s Sale) notifications(h *server.Hertz) {
	h.POST("/vouchers/:id/subscribe", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		v, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		var subscribed int64
		e = s.DB.QueryRowContext(ctx, `INSERT INTO voucher_subscription(voucher_id,user_id) SELECT voucher_id,$2 FROM tb_seckill_voucher WHERE voucher_id=$1 AND begin_time>now() ON CONFLICT(voucher_id,user_id) DO UPDATE SET user_id=excluded.user_id RETURNING voucher_id`, v, api.UserID(ctx)).Scan(&subscribed)
		return subscribed, e
	}))
	h.GET("/notifications", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		return store.List(ctx, s.DB, "SELECT id,content,created_at FROM trade_notice WHERE user_id=$1 ORDER BY id DESC LIMIT 50", api.UserID(ctx))
	}))
}
func (s Sale) reminders(ctx context.Context) error {
	_, e := s.DB.ExecContext(ctx, `INSERT INTO trade_notice(event_key,user_id,content)
 SELECT CONCAT('opening:',v.voucher_id,':',s.user_id),s.user_id,CONCAT('优惠券 ',v.voucher_id,' 即将开抢')
 FROM voucher_subscription s JOIN tb_seckill_voucher v ON s.voucher_id=v.voucher_id
 WHERE v.begin_time BETWEEN NOW() AND NOW()+interval '5 minutes' ON CONFLICT DO NOTHING`)
	return e
}

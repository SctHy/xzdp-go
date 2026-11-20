package voucher

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/dal/redis"
	"xzdp/biz/utils"
	"xzdp/conf"
	"xzdp/internal/trade"
)

type SeckillVoucherService struct {
	Context        context.Context
	RequestContext *app.RequestContext
}

func NewSeckillVoucherService(ctx context.Context, c *app.RequestContext) *SeckillVoucherService {
	return &SeckillVoucherService{ctx, c}
}
func (h *SeckillVoucherService) Run(id *int64) (*string, error) {
	user := utils.GetUser(h.Context)
	if user == nil {
		return nil, errors.New("请先登录")
	}
	s := trade.Sale{Redis: redis.RedisClient, Prefix: trade.Prefix(conf.GetEnv())}
	order, err := s.Reserve(h.Context, *id, user.ID, string(h.RequestContext.GetHeader("X-Seckill-Token")))
	if err != nil {
		return nil, err
	}
	return &order, nil
}

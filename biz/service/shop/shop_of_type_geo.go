package shop

import (
	"context"

	"xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
	shop "xzdp/biz/model/shop"

	"github.com/cloudwego/hertz/pkg/app"
)

type ShopOfTypeGeoService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewShopOfTypeGeoService(Context context.Context, RequestContext *app.RequestContext) *ShopOfTypeGeoService {
	return &ShopOfTypeGeoService{RequestContext: RequestContext, Context: Context}
}

func (h *ShopOfTypeGeoService) Run(req *shop.ShopOfTypeGeoReq) (resp *[]*shop.Shop, err error) {
	shops, err := redis.QueryShopWithDistance(h.Context, req)
	if err != nil {
		return nil, err
	}
	resp, err = postgres.QueryAllShop(h.Context, shops)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

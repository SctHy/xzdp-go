package shop

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"xzdp/biz/model/shop"
)

type ShopInfoService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewShopInfoService(Context context.Context, RequestContext *app.RequestContext) *ShopInfoService {
	return &ShopInfoService{RequestContext: RequestContext, Context: Context}
}

func (h *ShopInfoService) Run(id int64) (resp *shop.Shop, err error) {
	return shops.Detail(h.Context, id)
}

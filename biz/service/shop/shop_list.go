package shop

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"xzdp/biz/model/shop"
)

type ShopListService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewShopListService(Context context.Context, RequestContext *app.RequestContext) *ShopListService {
	return &ShopListService{RequestContext: RequestContext, Context: Context}
}

func (h *ShopListService) Run(req *shop.Empty) (resp *[]*shop.ShopType, err error) {
	data, err := shops.Types(h.Context)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

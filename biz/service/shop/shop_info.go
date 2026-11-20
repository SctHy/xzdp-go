package shop

import (
	"context"
	"encoding/json"

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

func (h *ShopInfoService) JSON(id int64) (json.RawMessage, error) {
	return shops.DetailJSON(h.Context, id)
}

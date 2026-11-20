package blog

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"strconv"
	"xzdp/biz/model/blog"
	"xzdp/biz/utils"
)

type LikeBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewLikeBlogService(Context context.Context, RequestContext *app.RequestContext) *LikeBlogService {
	return &LikeBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *LikeBlogService) Run(req *string) (resp *blog.LikeResp, err error) {
	id, err := strconv.ParseInt(*req, 10, 64)
	if err != nil {
		return nil, err
	}
	store, err := socialStore()
	if err != nil {
		return nil, err
	}
	liked, err := store.Toggle(h.Context, id, utils.GetUser(h.Context).GetID())
	if err != nil {
		return nil, err
	}
	return &blog.LikeResp{IsLiked: liked}, nil
}

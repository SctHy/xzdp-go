package blog

import (
	"context"
	"errors"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
	blog "xzdp/biz/model/blog"
	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"
)

type GetBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewGetBlogService(Context context.Context, RequestContext *app.RequestContext) *GetBlogService {
	return &GetBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *GetBlogService) Run(req *string) (resp *blog.Blog, err error) {
	if !errors.Is(postgres.DB.WithContext(h.Context).First(&resp, "id = ?", req).Error, nil) {
		return nil, errors.New("未找到该博客")
	}
	userId := resp.UserId
	user, err := postgres.GetById(h.Context, userId)
	if err != nil {
		return nil, err
	}
	resp.Icon = user.Icon
	resp.NickName = user.NickName
	if store, err := socialStore(); err == nil {
		resp.Liked = store.Count(h.Context, resp.ID, resp.Liked)
	}
	resp.IsLiked = false
	// 获取点赞状态
	viewer := utils.GetUser(h.Context)
	if viewer == nil {
		return resp, nil
	}
	u := viewer.GetID()
	key := constants.BLOG_LIKED_KEY + *req
	isLike, err := redis.IsLiked(h.Context, key, strconv.FormatInt(u, 10))
	if err != nil {
		return nil, err
	}
	resp.IsLiked = isLike
	return resp, nil
}

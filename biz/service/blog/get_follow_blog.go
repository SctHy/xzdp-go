package blog

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
	"xzdp/biz/model/blog"
	"xzdp/biz/utils"
	"xzdp/internal/social"
)

type GetFollowBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewGetFollowBlogService(Context context.Context, RequestContext *app.RequestContext) *GetFollowBlogService {
	return &GetFollowBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *GetFollowBlogService) Run(req *blog.FollowBlogReq) (resp *blog.FollowBlogRresp, err error) {
	return social.Timeline(h.Context, mysql.DB, redis.RedisClient, utils.GetUser(h.Context).GetID(), req)
}

package blog

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"xzdp/biz/dal/postgres"
	"xzdp/biz/model/blog"
	"xzdp/biz/utils"
	"xzdp/conf"
	"xzdp/internal/social"
)

type PostBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewPostBlogService(Context context.Context, RequestContext *app.RequestContext) *PostBlogService {
	return &PostBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *PostBlogService) Run(req *blog.Blog) (resp *blog.Blog, err error) {
	u := utils.GetUser(h.Context)
	req.UserId = u.GetID()
	if err := social.Publish(h.Context, postgres.DB, req, conf.GetConf().FeedThreshold); err != nil {
		return nil, err
	}
	req.Icon = u.Icon
	req.NickName = u.NickName
	return req, nil
}

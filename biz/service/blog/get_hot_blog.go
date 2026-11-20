package blog

import (
	"context"

	"xzdp/biz/dal/postgres"
	blog "xzdp/biz/model/blog"

	"github.com/cloudwego/hertz/pkg/app"
	"xzdp/biz/pkg/constants"
)

type GetHotBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewGetHotBlogService(Context context.Context, RequestContext *app.RequestContext) *GetHotBlogService {
	return &GetHotBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *GetHotBlogService) Run(req *blog.BlogReq) (resp *[]*blog.Blog, err error) {
	store, err := socialStore()
	if err != nil {
		return nil, err
	}
	ids, err := store.RankPage(h.Context, int(req.Current), constants.MAX_PAGE_SIZE)
	if err == nil {
		items, err := postgres.RankedBlogDetails(h.Context, ids)
		if err != nil {
			return nil, err
		}
		if len(items) == len(ids) {
			return &items, nil
		}
	}
	blogList, err := postgres.QueryHotBlog(h.Context, int(req.Current))
	if err != nil {
		return nil, err
	}
	return &blogList, nil
}

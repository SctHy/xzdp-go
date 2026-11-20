package blog

import (
	"context"

	"strconv"
	"xzdp/biz/dal/mysql"

	"github.com/cloudwego/hertz/pkg/app"
	blog "xzdp/biz/model/blog"
)

type DeleteBlogService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewDeleteBlogService(Context context.Context, RequestContext *app.RequestContext) *DeleteBlogService {
	return &DeleteBlogService{RequestContext: RequestContext, Context: Context}
}

func (h *DeleteBlogService) Run(req *string) (resp *blog.Empty, err error) {
	bid, err := strconv.ParseInt(*req, 10, 64)
	if err != nil {
		return nil, err
	}
	err = mysql.DeleteBlogComment(h.Context, bid)
	if err != nil {
		return nil, err
	}
	// 从数据库删除博客
	err = mysql.DB.WithContext(h.Context).Where("id = ?", bid).Delete(&blog.Blog{}).Error
	if err != nil {
		return nil, err
	}
	s, err := socialStore()
	if err != nil {
		return nil, err
	}
	return &blog.Empty{}, s.Forget(h.Context, bid)
}

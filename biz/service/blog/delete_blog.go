package blog

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"gorm.io/gorm"

	"xzdp/biz/dal/mysql"
	"xzdp/biz/model/blog"
	"xzdp/biz/utils"
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
	owner := utils.GetUser(h.Context).GetID()
	err = mysql.DB.WithContext(h.Context).Transaction(func(tx *gorm.DB) error {
		deleted := tx.Where("id=? AND user_id=?", bid, owner).Delete(&blog.Blog{})
		if deleted.Error != nil {
			return deleted.Error
		}
		if deleted.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		if err := tx.Exec("DELETE FROM tb_blog_comments WHERE blog_id=?", bid).Error; err != nil {
			return err
		}
		return tx.Exec("DELETE FROM feed_publication WHERE blog_id=?", bid).Error
	})
	if err != nil {
		return nil, err
	}
	s, err := socialStore()
	if err != nil {
		return nil, err
	}
	return &blog.Empty{}, s.Forget(h.Context, bid)
}

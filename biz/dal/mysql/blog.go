package mysql

import (
	"context"

	"github.com/cloudwego/hertz/pkg/common/hlog"

	"xzdp/biz/model/blog"
	"xzdp/biz/model/user"
	"xzdp/biz/pkg/constants"
)

func QueryBlogByUserID(ctx context.Context, current int, user *user.UserDTO) (resp []*blog.Blog, err error) {
	var blogs []*blog.Blog
	pageSize := constants.MAX_PAGE_SIZE
	err = DB.Where("user_id = ?", user.ID).Order("liked desc").Limit(pageSize).Offset((current - 1) * pageSize).Find(&blogs).Error
	if err != nil {
		hlog.CtxErrorf(ctx, "query error: %v", err)
		return nil, err
	}
	for i := range blogs {
		blogs[i].NickName = user.NickName
		blogs[i].Icon = user.Icon
	}

	return blogs, nil
}

func QueryHotBlog(ctx context.Context, current int) ([]*blog.Blog, error) {
	var rows []struct {
		blog.Blog
		AuthorName string
		AuthorIcon string
	}
	err := DB.WithContext(ctx).Raw(`SELECT b.*,u.nick_name AS author_name,u.icon AS author_icon
 FROM tb_blog b JOIN tb_user u ON u.id=b.user_id
 ORDER BY b.liked DESC,b.id DESC LIMIT ? OFFSET ?`, constants.MAX_PAGE_SIZE, (max(1, current)-1)*constants.MAX_PAGE_SIZE).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	result := make([]*blog.Blog, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		row.Blog.NickName = row.AuthorName
		row.Blog.Icon = row.AuthorIcon
		result = append(result, &row.Blog)
	}
	return result, nil
}

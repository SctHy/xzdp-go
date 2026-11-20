package user

import (
	"context"
	"strconv"

	"xzdp/biz/dal/postgres"
	user "xzdp/biz/model/user"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"gorm.io/gorm"
)

type UserInfoService struct {
	RequestContext *app.RequestContext
	Context        context.Context
	DB             *gorm.DB
}

func NewUserInfoService(ctx context.Context, requestContext *app.RequestContext) *UserInfoService {
	return &UserInfoService{
		RequestContext: requestContext,
		Context:        ctx,
		DB:             postgres.DB,
	}
}

// NewUserInfoServiceWithDB is used for testing
func NewUserInfoServiceWithDB(ctx context.Context, requestContext *app.RequestContext, db *gorm.DB) *UserInfoService {
	return &UserInfoService{
		RequestContext: requestContext,
		Context:        ctx,
		DB:             db,
	}
}

func (h *UserInfoService) Run(req *user.UserLoginFrom, c *app.RequestContext) (resp *user.UserInfo, err error) {
	strId := c.Param("id")
	id, err := strconv.ParseInt(strId, 10, 64)
	if err != nil {
		return nil, err
	}
	userInfo, err := postgres.GetUserInfoById(h.Context, h.DB, id)
	if err == nil && userInfo != nil {
		return userInfo, nil
	}

	err = h.createNewUserWithId(id)
	if err != nil {
		return nil, err
	}
	userInfo, err = postgres.GetUserInfoById(h.Context, h.DB, id)
	if err != nil {
		return nil, err
	}
	return userInfo, nil
}

func (h *UserInfoService) createNewUserWithId(id int64) error {
	user := user.UserInfo{
		UserId: id,
	}
	result := h.DB.WithContext(h.Context).Omit("Birthday").Create(&user)
	hlog.CtxDebugf(h.Context, "result = %+v", result)
	return result.Error
}

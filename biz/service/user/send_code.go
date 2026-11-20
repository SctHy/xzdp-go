package user

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"
	"xzdp/biz/dal/redis"
	user "xzdp/biz/model/user"
	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

type SendCodeService struct {
	RequestContext *app.RequestContext
	Context        context.Context
}

func NewSendCodeService(Context context.Context, RequestContext *app.RequestContext) *SendCodeService {
	return &SendCodeService{RequestContext: RequestContext, Context: Context}
}

func (h *SendCodeService) Run(req *user.UserLoginFrom) (resp *user.Result, err error) {

	phone := req.Phone

	if !utils.ValidateMobile(phone) {
		return nil, errors.New("phone isn't validate")
	}
	if phone == "" {
		return nil, errors.New("phone can't be empty")
	}

	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return nil, err
	}
	code := fmt.Sprintf("%06d", n.Int64())
	err = redis.RedisClient.Set(h.Context, constants.LOGIN_CODE_KEY+phone, code, constants.LOGIN_CODE_EXPIRE*time.Second).Err() // add expiration
	if err != nil {
		hlog.CtxErrorf(h.Context, "err = %s", err.Error())
		return nil, err
	}

	return &user.Result{Success: true}, nil
}

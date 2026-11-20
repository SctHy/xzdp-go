package interceptor

import (
	"context"
	"fmt"
	"time"
	"xzdp/biz/dal/redis"
	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

func UniqueVisitor(ctx context.Context, c *app.RequestContext) {

	userDTO := utils.GetUser(ctx)
	if userDTO == nil {
		c.Next(ctx)
		return
	}

	now := time.Now()

	// 格式化为 YYYY-MM-DD 格式
	today := now.Format("2006-01-02")
	hhlVal := fmt.Sprint(userDTO.ID) + today
	err := redis.RedisClient.PFAdd(ctx, constants.HLL_UV_KEY, hhlVal).Err()
	if err != nil {
		hlog.CtxErrorf(ctx, "PFAdd error: %v", err)
	}
	c.Next(ctx)
}

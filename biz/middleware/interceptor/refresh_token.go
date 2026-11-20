package interceptor

import (
	"context"
	"errors"
	"github.com/cloudwego/hertz/pkg/app"
	"time"
	"xzdp/biz/dal/redis"
	"xzdp/biz/model/user"
	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"
)

func publicRoute(method, path string) bool {
	if method == "POST" {
		return path == "/user/login" || path == "/user/code"
	}
	if method != "GET" {
		return false
	}
	switch path {
	case "/ping", "/hello", "/shop/:id", "/shop/of/type", "/shop/of/type/geo", "/shop-type/list", "/voucher/list/:id", "/blog/hot", "/blog/:id", "/blog/user/:id", "/blog/likes/:id", "/imgs/*filepath":
		return true
	}
	return false
}

func CheckToken(ctx context.Context, c *app.RequestContext) {
	path := c.FullPath()
	switch path {
	case "/health/live", "/health/ready", "/metrics":
		c.Next(ctx)
		return
	}
	token := string(c.GetHeader("authorization"))
	if token != "" {
		var u user.UserDTO
		key := constants.LOGIN_USER_KEY + token
		if err := redis.RedisClient.HGetAll(ctx, key).Scan(&u); err != nil {
			utils.SendErrResponse(ctx, c, 503, errors.New("session store unavailable"))
			c.Abort()
			return
		}
		if u.ID > 0 {
			ctx = utils.SaveUser(ctx, &u)
			redis.RedisClient.Expire(ctx, key, time.Minute)
		}
	}
	if !publicRoute(string(c.Method()), path) && utils.GetUser(ctx) == nil {
		utils.SendErrResponse(ctx, c, 401, errors.New("请先登录"))
		c.Abort()
		return
	}
	c.Next(ctx)
}

package interceptor

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

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
		sessionCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		pipe := redis.RedisClient.Pipeline()
		fields := pipe.HGetAll(sessionCtx, key)
		pipe.Expire(sessionCtx, key, time.Minute)
		_, err := pipe.Exec(sessionCtx)
		if err == nil {
			err = fields.Scan(&u)
		}
		cancel()
		if err != nil {
			// Shop reads are viewer-independent and may fall back to MySQL.
			if string(c.Method()) == "GET" && (path == "/shop/:id" || path == "/shop-type/list") {
				c.Next(ctx)
				return
			}
			utils.SendErrResponse(ctx, c, 503, errors.New("session store unavailable"))
			c.Abort()
			return
		}
		if u.ID > 0 {
			ctx = utils.SaveUser(ctx, &u)
		}
	}
	if !publicRoute(string(c.Method()), path) && utils.GetUser(ctx) == nil {
		utils.SendErrResponse(ctx, c, 401, errors.New("请先登录"))
		c.Abort()
		return
	}
	c.Next(ctx)
}

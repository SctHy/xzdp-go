package interceptor

import (
	"context"
	"encoding/json"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"testing"
)

func TestRouteAccess(t *testing.T) {
	h := server.New()
	h.Use(Access, CheckToken)
	ok := func(_ context.Context, c *app.RequestContext) { c.Status(200) }
	h.GET("/shop/:id", ok)
	h.POST("/voucher-order/seckill/:id", ok)
	h.GET("/user/me", ok)
	for _, tc := range []struct {
		method, path string
		want         int
	}{{"GET", "/shop/1", 200}, {"POST", "/voucher-order/seckill/1", 401}, {"GET", "/user/me", 401}} {
		res := ut.PerformRequest(h.Engine, tc.method, tc.path, nil).Result()
		if res.StatusCode() != tc.want || len(res.Header.Peek("X-Request-ID")) == 0 {
			t.Fatalf("%s: %s", tc.path, res.Body())
		}
		if tc.want == 401 {
			var v struct {
				Success                   bool
				Code, ErrorMsg, RequestId string
			}
			if json.Unmarshal(res.Body(), &v) != nil || v.Success || v.Code != "UNAUTHENTICATED" || v.ErrorMsg == "" || v.RequestId == "" {
				t.Fatalf("%s", res.Body())
			}
		}
	}
}

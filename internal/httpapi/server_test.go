package httpapi

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/redis/go-redis/v9"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/store"
)

func TestHealth(t *testing.T) {
	h := server.New()
	var draining atomic.Bool
	var readinessErr error
	calls := 0
	registerHealth(h, func(context.Context) error { calls++; return readinessErr }, &draining)
	check := func(path string, want int) {
		t.Helper()
		if got := ut.PerformRequest(h.Engine, "GET", path, nil).Code; got != want {
			t.Fatalf("%s: got %d, want %d", path, got, want)
		}
	}
	check("/livez", 200)
	check("/readyz", 200)
	readinessErr = errors.New("offline")
	check("/readyz", 503)
	readinessErr = nil
	draining.Store(true)
	check("/readyz", 503)
	check("/livez", 200)
	if calls != 2 {
		t.Fatalf("dependency probes = %d", calls)
	}
}

func TestMiddleware(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	defer client.Close()
	if err := r.Set("auth:session:valid", "42"); err != nil {
		t.Fatal(err)
	}
	h := server.New()
	h.Use(middleware(&store.Store{Redis: client}))
	handler := func(ctx context.Context, c *app.RequestContext) { c.String(200, "%d", api.UserID(ctx)) }
	h.GET("/notes/:id", handler)
	h.POST("/notes", handler)
	h.POST("/user/email/code", handler)
	for _, tc := range []struct {
		method, path, token string
		status              int
		body                string
	}{
		{"GET", "/notes/4", "", 200, "0"},
		{"POST", "/user/email/code", "", 200, "0"},
		{"POST", "/notes", "", 401, ""},
		{"POST", "/notes", "Bearer expired", 401, ""},
		{"POST", "/notes", "Bearer valid", 200, "42"},
	} {
		got := ut.PerformRequest(h.Engine, tc.method, tc.path, nil, ut.Header{Key: "Authorization", Value: tc.token})
		if got.Code != tc.status || (tc.status == 200 && got.Body.String() != tc.body) {
			t.Fatalf("%s %s returned %d %s", tc.method, tc.path, got.Code, got.Body)
		}
	}
}

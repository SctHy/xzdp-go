package httpapi

import (
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/redis/go-redis/v9"

	"xzdp/internal/cache"
	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/metrics"
	"xzdp/internal/shop"
	"xzdp/internal/social"
	"xzdp/internal/store"
	"xzdp/internal/trade"
	"xzdp/internal/user"
)

//go:embed web
var web embed.FS

func static(name string) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		file := name
		if file == "" {
			file = "assets/" + strings.TrimPrefix(c.Param("file"), "/")
		}
		b, e := web.ReadFile("web/" + file)
		if e != nil {
			c.Status(404)
			return
		}
		c.Data(200, mime.TypeByExtension(path.Ext(file)), b)
	}
}

var rateLimitScript = redis.NewScript("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],1) end return n")

func middleware(s *store.Store) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		route := c.FullPath()
		public := route == "/user/email/login" || route == "/user/email/code" || string(c.Method()) == "GET" && (strings.HasPrefix(route, "/shops/") || route == "/vouchers" || route == "/categories" || strings.HasPrefix(route, "/notes"))
		token := strings.TrimPrefix(string(c.GetHeader("Authorization")), "Bearer ")
		id := int64(0)
		if token != "" {
			var e error
			id, e = s.Redis.Get(ctx, s.Prefix+"auth:session:"+token).Int64()
			if e != nil && !errors.Is(e, redis.Nil) {
				api.Fail(c, 503, "会话服务不可用")
				c.Abort()
				return
			}
		}
		if id == 0 && !public {
			api.Fail(c, 401, "请先登录")
			c.Abort()
			return
		}
		addr := c.RemoteAddr().String()
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		k := s.Prefix + "rate:" + host
		n, e := rateLimitScript.Run(ctx, s.Redis, []string{k}).Int()
		if e != nil || n > 300 {
			api.Fail(c, 429, "请稍后重试")
			c.Abort()
			return
		}
		day := s.Prefix + "uv:" + time.Now().UTC().Format("20060102")
		pipe := s.Redis.Pipeline()
		pipe.PFAdd(ctx, day, host)
		pipe.Expire(ctx, day, 48*time.Hour)
		_, _ = pipe.Exec(ctx)
		c.Next(api.WithUser(ctx, id))
	}
}

func Register(h *server.Hertz, s *store.Store, sale trade.Sale) {
	h.GET("/", static("index.html"))
	h.GET("/assets/*file", static(""))
	h.Use(middleware(s))
	shop.Register(h, s.DB)
	shop.RegisterBookings(h, s.DB)
	shop.RegisterDetail(h, s.DB, cache.New(s.Redis, s.Prefix+"shop:", 512, time.Minute))
	social.Register(h, s.DB)
	sale.Register(h, os.Getenv("MOCK_PAYMENT") == "true")
	u := user.Service{DB: s.DB, Redis: s.Redis, Prefix: s.Prefix + "auth:", SMTP: user.SMTPConfig(), SMS: user.SMS{URL: os.Getenv("SMS_WEBHOOK_URL"), Token: os.Getenv("SMS_WEBHOOK_TOKEN")}, Demo: os.Getenv("DEMO_CODES") == "true"}
	u.Register(h)
	user.RegisterSign(h, s.DB, s.Redis, s.Prefix)
}
func registerHealth(h *server.Hertz, ready func(context.Context) error, draining *atomic.Bool) {
	h.GET("/livez", func(ctx context.Context, c *app.RequestContext) { c.String(200, "ok") })
	h.GET("/readyz", func(ctx context.Context, c *app.RequestContext) {
		if draining.Load() || ready(ctx) != nil || draining.Load() {
			c.Status(503)
			return
		}
		c.String(200, "ready")
	})
}

func Run(worker bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	s, e := store.Open(ctx)
	if e != nil {
		return e
	}
	defer s.Close()
	b, e := strconv.Atoi(store.Env("STOCK_BUCKETS", "1"))
	if e != nil || b < 1 || b > 64 {
		return fmt.Errorf("STOCK_BUCKETS must be 1..64")
	}
	sale := trade.Sale{DB: s.DB, Redis: s.Redis, Prefix: s.Prefix + "trade:", Buckets: b}
	addr := ":8080"
	if worker {
		addr = ":8082"
	}
	h := server.New(server.WithHostPorts(store.Env("LISTEN_ADDR", addr)), server.WithExitWaitTime(10*time.Second))
	sale.Metrics = metrics.Register(h)
	var draining atomic.Bool
	registerHealth(h, s.Ready, &draining)
	if !worker {
		Register(h, s, sale)
	} else if e = sale.InitGroup(ctx); e != nil {
		return e
	}
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	if worker {
		go func() { done <- sale.Run(run, rand.Text()) }()
	}
	httpDone := make(chan error, 1)
	go func() { httpDone <- h.Run() }()
	select {
	case <-ctx.Done():
	case e = <-httpDone:
	case e = <-done:
		worker = false
	}
	draining.Store(true)
	slog.Info("draining")
	shutdown, end := context.WithTimeout(context.Background(), 12*time.Second)
	defer end()
	closeErr := h.Shutdown(shutdown)
	cancel()
	if worker {
		<-done
	}
	return errors.Join(e, closeErr)
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"golang.org/x/sync/errgroup"

	"xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
	"xzdp/biz/handler/health"
	"xzdp/biz/middleware/interceptor"
	shopservice "xzdp/biz/service/shop"
	"xzdp/conf"
	"xzdp/internal/migrate"
	"xzdp/internal/observe"
	"xzdp/internal/social"
	"xzdp/internal/trade"
)

type frameworkLog struct{}

func (frameworkLog) Write(b []byte) (int, error) {
	slog.Info("hertz", "message", strings.TrimSpace(string(b)))
	return len(b), nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := conf.Current()
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.Hertz.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	hlog.SetOutput(frameworkLog{})
	hlog.SetLevel(conf.LogLevel())
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pool, err := mysql.Open(startup)
	if err != nil {
		return fmt.Errorf("open MySQL: %w", err)
	}
	defer pool.Close()
	if err = redis.Open(startup); err != nil {
		return fmt.Errorf("open Redis: %w", err)
	}
	defer redis.RedisClient.Close()
	if err = migrate.Apply(ctx, pool); err != nil {
		return err
	}
	sale := trade.Sale{DB: pool, Redis: redis.RedisClient, Prefix: trade.Prefix(cfg.Env)}
	if err = sale.Preheat(ctx); err != nil {
		return err
	}
	shopCache := shopservice.ConfigureCache(cfg)
	workers, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	socialStore := social.Store{DB: pool, Redis: redis.RedisClient, Prefix: "xzdp:" + cfg.Env + ":social:"}
	visitors := interceptor.NewVisitors(redis.RedisClient)
	var cacheGroup errgroup.Group
	cacheGroup.Go(func() error { return visitors.Run(workers) })
	cacheGroup.Go(func() error { return socialStore.Run(workers) })
	cacheGroup.Go(func() error { return sale.Maintain(workers) })
	cacheGroup.Go(func() error { return sale.Run(workers, cfg.KafkaBroker, cfg.Env) })
	cacheGroup.Go(func() error { shopCache.Subscribe(workers); return nil })
	cacheGroup.Go(func() error { shopCache.Warm(workers); return nil })
	defer func() { stopWorkers(); cacheGroup.Wait() }()
	checks := &health.Checks{MySQL: pool.PingContext, Redis: func(ctx context.Context) error { return redis.RedisClient.Ping(ctx).Err() }}
	h := server.New(server.WithHostPorts(cfg.Hertz.Address))
	streams, endStreams := context.WithCancel(context.Background())
	defer endStreams()
	h.Use(interceptor.Access, interceptor.Cors, interceptor.CheckToken, sale.Limit, visitors.Record)
	h.Use(func(ctx context.Context, c *app.RequestContext) {
		if c.FullPath() == "/message/sse" {
			streamCtx, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(streams, cancel)
			defer stop()
			defer cancel()
			ctx = streamCtx
		}
		c.Next(ctx)
	})
	h.GET("/health/live", health.Live)
	h.GET("/health/ready", checks.Ready)
	h.GET("/metrics", func(_ context.Context, c *app.RequestContext) {
		metrics, err := observe.Text()
		if err != nil {
			c.Status(500)
			return
		}
		c.Data(200, "text/plain; version=0.0.4", []byte("# TYPE xzdp_build_info gauge\nxzdp_build_info 1\n"+shopCache.Metrics()+metrics))
	})
	sale.Register(h)
	register(h)
	if cfg.Hertz.EnablePprof {
		listener, err := net.Listen("tcp", "127.0.0.1:6060")
		if err != nil {
			return err
		}
		profiler := &http.Server{ReadHeaderTimeout: 3 * time.Second}
		defer profiler.Close()
		go func() {
			if err := profiler.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("pprof", "error", err)
			}
		}()
	}
	done := make(chan error, 1)
	go func() { done <- h.Run() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	checks.Draining.Store(true)
	endStreams()
	drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	return h.Shutdown(drain)
}

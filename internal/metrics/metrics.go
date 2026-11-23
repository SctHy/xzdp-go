package metrics

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct{ mq *prometheus.CounterVec }

func (m *Metrics) MQ(event string, n int) {
	if m == nil {
		return
	}
	switch event {
	case "produce", "consume", "reconcile", "dead_letter":
		m.mq.WithLabelValues(event).Add(float64(n))
	}
}
func Register(h *server.Hertz) *Metrics {
	return register(h, prometheus.NewRegistry())
}

func register(h *server.Hertz, r *prometheus.Registry) *Metrics {
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "xzdp_http_seconds", Help: "Request duration by registered route and status class."}, []string{"route", "status"})
	m := &Metrics{prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xzdp_mq_messages_total", Help: "Messages processed by operation."}, []string{"operation"})}
	r.MustRegister(latency, m.mq)
	for _, event := range []string{"produce", "consume", "reconcile", "dead_letter"} {
		m.MQ(event, 0)
	}
	h.Use(func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		latency.WithLabelValues(route, strconv.Itoa(c.Response.StatusCode()/100)+"xx").Observe(time.Since(start).Seconds())
	})
	handler := promhttp.HandlerFor(r, promhttp.HandlerOpts{})
	h.GET("/metrics", adaptor.HertzHandler(handler))
	return m
}

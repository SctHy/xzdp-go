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

type Metrics struct{ stream *prometheus.CounterVec }

func (m *Metrics) Stream(event string, n int) {
	if m == nil {
		return
	}
	switch event {
	case "ack", "reclaim", "dead_letter":
		m.stream.WithLabelValues(event).Add(float64(n))
	}
}
func Register(h *server.Hertz) *Metrics {
	return register(h, prometheus.NewRegistry())
}

func register(h *server.Hertz, r *prometheus.Registry) *Metrics {
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "xzdp_http_seconds", Help: "Request duration by registered route and status class."}, []string{"route", "status"})
	m := &Metrics{prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xzdp_stream_messages_total", Help: "Messages processed by operation."}, []string{"operation"})}
	r.MustRegister(latency, m.stream)
	for _, event := range []string{"ack", "reclaim", "dead_letter"} {
		m.Stream(event, 0)
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

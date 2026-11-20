// Package observe exposes process-local metrics through the API's Prometheus endpoint.
package observe

import (
	"bytes"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

var registry = prometheus.NewRegistry()
var requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xzdp_http_requests_total", Help: "Completed HTTP requests by registered route and status."}, []string{"route", "status"})
var latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "xzdp_http_request_duration_seconds", Help: "Time to complete the HTTP handler.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 5}}, []string{"route"})
var LuaRejections = prometheus.NewCounter(prometheus.CounterOpts{Name: "xzdp_lua_rejections_total", Help: "Business and rate-limit rejections returned by admission scripts."})
var KafkaLag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "xzdp_kafka_consumer_lag", Help: "Sum of order topic end offsets minus the DB consumer group's committed offsets."})
var LagSampleTime = prometheus.NewGauge(prometheus.GaugeOpts{Name: "xzdp_kafka_lag_last_success_timestamp_seconds", Help: "Unix time of last successful broker sample."})

func init() {
	registry.MustRegister(requests, latency, LuaRejections, KafkaLag, LagSampleTime)
	KafkaLag.Set(-1)
}

func HTTP(route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	requests.WithLabelValues(route, strconv.Itoa(status)).Inc()
	latency.WithLabelValues(route).Observe(duration.Seconds())
}

func Text() (string, error) {
	families, err := registry.Gather()
	if err != nil {
		return "", err
	}
	var body bytes.Buffer
	for _, family := range families {
		if _, err = expfmt.MetricFamilyToText(&body, family); err != nil {
			return "", err
		}
	}
	return body.String(), nil
}

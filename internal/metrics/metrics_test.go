package metrics

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRegisteredMetricLabels(t *testing.T) {
	h := server.New()
	registry := prometheus.NewRegistry()
	m := register(h, registry)
	h.GET("/orders/:id", func(ctx context.Context, c *app.RequestContext) { c.Status(201) })
	for _, url := range []string{"/orders/123", "/orders/456"} {
		ut.PerformRequest(h.Engine, "GET", url, nil)
	}
	m.MQ("produce", 2)
	m.MQ("user_123", 1)
	gathered, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := make(map[string]*dto.MetricFamily)
	for _, family := range gathered {
		families[family.GetName()] = family
	}
	hist := families["xzdp_http_seconds"].GetMetric()
	if len(hist) != 1 || hist[0].GetHistogram().GetSampleCount() != 2 {
		t.Fatal(hist)
	}
	labels := hist[0].GetLabel()
	if len(labels) != 2 || labels[0].GetName() != "route" || labels[0].GetValue() != "/orders/:id" || labels[1].GetName() != "status" || labels[1].GetValue() != "2xx" {
		t.Fatal(labels)
	}
	counts := map[string]float64{"produce": 2, "consume": 0, "reconcile": 0, "dead_letter": 0}
	for _, m := range families["xzdp_mq_messages_total"].GetMetric() {
		labels := m.GetLabel()
		if len(labels) != 1 || labels[0].GetName() != "operation" {
			t.Fatal(labels)
		}
		op := labels[0].GetValue()
		if want, ok := counts[op]; !ok || want != m.GetCounter().GetValue() {
			t.Fatalf("unexpected counter %s", m)
		}
		delete(counts, op)
	}
	if len(counts) != 0 {
		t.Fatalf("missing counters %v", counts)
	}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				switch label.GetName() {
				case "user_id", "shop_id", "order_id":
					t.Fatalf("%s exposes %s", f.GetName(), label.GetName())
				}
			}
		}
	}
}

package observe

import (
	"testing"
	"time"
	"strings"

	"github.com/prometheus/common/expfmt"
)

func TestMetricsAreScrapable(t *testing.T) {
	HTTP("/shop/:id", 200, 20*time.Millisecond)
	text, err := Text()
	if err != nil {
		t.Fatal(err)
	}
	var parser expfmt.TextParser
	metrics, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	histogram := metrics["xzdp_http_request_duration_seconds"].GetMetric()[0].GetHistogram()
	if histogram.GetSampleCount() < 1 || histogram.GetSampleSum() <= 0 {
		t.Fatal(histogram)
	}
}

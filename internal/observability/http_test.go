package observability

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsHandlerReturnsPrometheusText(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	r := httptest.NewRecorder()
	MetricsHandler(m).ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	if r.Code != 200 || !strings.Contains(r.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("response = %d %q", r.Code, r.Header().Get("Content-Type"))
	}
}

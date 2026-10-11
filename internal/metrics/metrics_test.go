package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsRegistry(t *testing.T) {
	reg := NewRegistry()

	c := NewCounter("test_counter", "A test counter", `env="test"`)
	g := NewGauge("test_gauge", "A test gauge")
	h := NewHistogram("test_histogram", "A test histogram", []float64{0.001, 0.010, 0.100})

	reg.RegisterCounter(c)
	reg.RegisterGauge(g)
	reg.RegisterHistogram(h)

	c.Inc()
	c.Add(4)
	if c.Value() != 5 {
		t.Fatalf("expected counter 5, got %d", c.Value())
	}

	g.Set(42)
	g.Add(-10)
	if g.Value() != 32 {
		t.Fatalf("expected gauge 32, got %d", g.Value())
	}

	timer := NewTimer(h)
	time.Sleep(2 * time.Millisecond)
	timer.ObserveDuration()

	var buf bytes.Buffer
	if err := reg.WritePrometheusText(&buf); err != nil {
		t.Fatalf("WritePrometheusText failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `# TYPE test_counter counter`) {
		t.Errorf("missing counter type in output: %s", out)
	}
	if !strings.Contains(out, `test_counter{env="test"} 5`) {
		t.Errorf("missing counter value in output: %s", out)
	}
	if !strings.Contains(out, `test_gauge 32`) {
		t.Errorf("missing gauge value in output: %s", out)
	}
	if !strings.Contains(out, `test_histogram_count 1`) {
		t.Errorf("missing histogram count in output: %s", out)
	}
}

func TestMetricsHTTPHandler(t *testing.T) {
	em := DefaultEngineMetrics
	em.TCPRequestsPutOk.Inc()
	em.MemTableActiveBytes.Set(1024)

	handler := em.Registry.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body := w.Body.String()
	if !strings.Contains(body, "forest_tcp_requests_total") {
		t.Errorf("expected forest_tcp_requests_total in output: %s", body)
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewHistogram("bench_hist", "Benchmark histogram", nil)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.Observe(0.0005)
	}
}

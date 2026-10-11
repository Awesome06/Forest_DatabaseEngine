// Package metrics provides a zero-allocation, thread-safe Prometheus metrics registry
// and exposition handler using pure Go standard library primitives.
// It adheres to strict zero-dependency and low-latency systems engineering constraints.
package metrics

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing 64-bit integer metric.
type Counter struct {
	name   string
	help   string
	labels string
	val    atomic.Uint64
}

// NewCounter creates and registers a Counter.
func NewCounter(name, help string, labels string) *Counter {
	return &Counter{
		name:   name,
		help:   help,
		labels: labels,
	}
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.val.Add(1)
}

// Add adds delta to the counter.
func (c *Counter) Add(delta uint64) {
	c.val.Add(delta)
}

// Value returns the current value of the counter.
func (c *Counter) Value() uint64 {
	return c.val.Load()
}

// Gauge represents a numerical value that can arbitrarily increase and decrease.
type Gauge struct {
	name string
	help string
	val  atomic.Int64
}

// NewGauge creates and registers a Gauge.
func NewGauge(name, help string) *Gauge {
	return &Gauge{
		name: name,
		help: help,
	}
}

// Set sets the gauge to the given value.
func (g *Gauge) Set(v int64) {
	g.val.Store(v)
}

// Add adds delta to the gauge value.
func (g *Gauge) Add(delta int64) {
	g.val.Add(delta)
}

// Value returns the current gauge value.
func (g *Gauge) Value() int64 {
	return g.val.Load()
}

// DefaultHistogramBuckets defines latency bucket upper bounds in seconds.
var DefaultHistogramBuckets = []float64{
	0.000001, // 1µs
	0.000005, // 5µs
	0.000010, // 10µs
	0.000050, // 50µs
	0.000100, // 100µs
	0.000500, // 500µs
	0.001000, // 1ms
	0.005000, // 5ms
	0.010000, // 10ms
	0.050000, // 50ms
	0.100000, // 100ms
	0.500000, // 500ms
	1.000000, // 1s
}

// Histogram tracks the statistical distribution of observed durations/quantities.
type Histogram struct {
	name    string
	help    string
	buckets []float64
	counts  []atomic.Uint64
	count   atomic.Uint64
	sumBits atomic.Uint64 // float64 encoded via math.Float64bits
}

// NewHistogram creates a new Histogram with specified bucket thresholds.
func NewHistogram(name, help string, buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultHistogramBuckets
	}
	return &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]atomic.Uint64, len(buckets)),
	}
}

// Observe records a value in seconds into the histogram buckets.
func (h *Histogram) Observe(v float64) {
	h.count.Add(1)

	// Update atomic float sum using CAS
	for {
		oldBits := h.sumBits.Load()
		newVal := math.Float64frombits(oldBits) + v
		newBits := math.Float64bits(newVal)
		if h.sumBits.CompareAndSwap(oldBits, newBits) {
			break
		}
	}

	for i, bound := range h.buckets {
		if v <= bound {
			h.counts[i].Add(1)
		}
	}
}

// Timer is a helper for timing operations.
type Timer struct {
	h     *Histogram
	start time.Time
}

// NewTimer creates a new timer.
func NewTimer(h *Histogram) Timer {
	return Timer{
		h:     h,
		start: time.Now(),
	}
}

// ObserveDuration records elapsed duration since timer creation into the histogram.
func (t Timer) ObserveDuration() {
	if t.h != nil {
		d := time.Since(t.start).Seconds()
		t.h.Observe(d)
	}
}

// Registry stores all registered metrics for exposition.
type Registry struct {
	mu         sync.RWMutex
	counters   []*Counter
	gauges     []*Gauge
	histograms []*Histogram
}

// NewRegistry initializes an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make([]*Counter, 0, 16),
		gauges:     make([]*Gauge, 0, 16),
		histograms: make([]*Histogram, 0, 16),
	}
}

// RegisterCounter adds a counter to the registry.
func (r *Registry) RegisterCounter(c *Counter) {
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
}

// RegisterGauge adds a gauge to the registry.
func (r *Registry) RegisterGauge(g *Gauge) {
	r.mu.Lock()
	r.gauges = append(r.gauges, g)
	r.mu.Unlock()
}

// RegisterHistogram adds a histogram to the registry.
func (r *Registry) RegisterHistogram(h *Histogram) {
	r.mu.Lock()
	r.histograms = append(r.histograms, h)
	r.mu.Unlock()
}

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// WritePrometheusText exports all registered metrics into the standard Prometheus exposition format.
func (r *Registry) WritePrometheusText(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Counters
	for _, c := range r.counters {
		buf.WriteString("# HELP ")
		buf.WriteString(c.name)
		buf.WriteString(" ")
		buf.WriteString(c.help)
		buf.WriteString("\n# TYPE ")
		buf.WriteString(c.name)
		buf.WriteString(" counter\n")
		buf.WriteString(c.name)
		if c.labels != "" {
			buf.WriteString("{")
			buf.WriteString(c.labels)
			buf.WriteString("}")
		}
		buf.WriteString(" ")
		buf.WriteString(strconv.FormatUint(c.Value(), 10))
		buf.WriteString("\n\n")
	}

	// Gauges
	for _, g := range r.gauges {
		buf.WriteString("# HELP ")
		buf.WriteString(g.name)
		buf.WriteString(" ")
		buf.WriteString(g.help)
		buf.WriteString("\n# TYPE ")
		buf.WriteString(g.name)
		buf.WriteString(" gauge\n")
		buf.WriteString(g.name)
		buf.WriteString(" ")
		buf.WriteString(strconv.FormatInt(g.Value(), 10))
		buf.WriteString("\n\n")
	}

	// Histograms
	for _, h := range r.histograms {
		buf.WriteString("# HELP ")
		buf.WriteString(h.name)
		buf.WriteString(" ")
		buf.WriteString(h.help)
		buf.WriteString("\n# TYPE ")
		buf.WriteString(h.name)
		buf.WriteString(" histogram\n")

		var cumCount uint64
		for i, bound := range h.buckets {
			cumCount = h.counts[i].Load()
			buf.WriteString(h.name)
			buf.WriteString(`_bucket{le="`)
			buf.WriteString(strconv.FormatFloat(bound, 'f', -1, 64))
			buf.WriteString(`"} `)
			buf.WriteString(strconv.FormatUint(cumCount, 10))
			buf.WriteString("\n")
		}

		totalCount := h.count.Load()
		buf.WriteString(h.name)
		buf.WriteString(`_bucket{le="+Inf"} `)
		buf.WriteString(strconv.FormatUint(totalCount, 10))
		buf.WriteString("\n")

		sum := math.Float64frombits(h.sumBits.Load())
		buf.WriteString(h.name)
		buf.WriteString("_sum ")
		buf.WriteString(strconv.FormatFloat(sum, 'f', -1, 64))
		buf.WriteString("\n")

		buf.WriteString(h.name)
		buf.WriteString("_count ")
		buf.WriteString(strconv.FormatUint(totalCount, 10))
		buf.WriteString("\n\n")
	}

	_, err := w.Write(buf.Bytes())
	return err
}

// Handler returns an HTTP handler serving Prometheus text format on /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.WritePrometheusText(w); err != nil {
			http.Error(w, fmt.Sprintf("failed writing metrics: %v", err), http.StatusInternalServerError)
		}
	})
}

// Package metrics defines the engine-specific Prometheus instrumentation instances.
package metrics

// EngineMetrics groups all production observability instruments for Forest.
type EngineMetrics struct {
	Registry *Registry

	// TCP Ingress Metrics
	TCPRequestsPutOk    *Counter
	TCPRequestsGetOk    *Counter
	TCPRequestsDelOk    *Counter
	TCPRequestsEchoOk   *Counter
	TCPRequestsScanOk   *Counter
	TCPRequestsErrors   *Counter
	TCPParseDuration    *Histogram

	// UDP Telemetry Ingress Metrics
	UDPPacketsTotal       *Counter
	UDPPacketsDropped     *Counter
	UDPTelemetryDuration  *Histogram

	// LSM Engine Internals
	MemTableSwapEvents   *Counter
	MemTableSwapDuration *Histogram
	MemTableActiveBytes  *Gauge
	WALFsyncTotal        *Counter
	WALFsyncDuration     *Histogram
}

// DefaultEngineMetrics is the globally accessible metrics bundle.
var DefaultEngineMetrics = NewEngineMetrics()

// NewEngineMetrics initializes and registers all standard Forest metrics.
func NewEngineMetrics() *EngineMetrics {
	reg := NewRegistry()

	em := &EngineMetrics{
		Registry: reg,

		TCPRequestsPutOk:    NewCounter("forest_tcp_requests_total", "Total TCP Put requests succeeded", `cmd="put",status="ok"`),
		TCPRequestsGetOk:    NewCounter("forest_tcp_requests_total", "Total TCP Get requests succeeded", `cmd="get",status="ok"`),
		TCPRequestsDelOk:    NewCounter("forest_tcp_requests_total", "Total TCP Delete requests succeeded", `cmd="delete",status="ok"`),
		TCPRequestsEchoOk:   NewCounter("forest_tcp_requests_total", "Total TCP Echo requests succeeded", `cmd="echo",status="ok"`),
		TCPRequestsScanOk:   NewCounter("forest_tcp_requests_total", "Total TCP Scan requests succeeded", `cmd="scan",status="ok"`),
		TCPRequestsErrors:   NewCounter("forest_tcp_requests_total", "Total TCP requests failed", `status="error"`),
		TCPParseDuration:    NewHistogram("forest_parse_duration_seconds", "Latency distribution of frame parsing and dispatch", nil),

		UDPPacketsTotal:      NewCounter("forest_udp_packets_total", "Total UDP telemetry datagrams received", ""),
		UDPPacketsDropped:    NewCounter("forest_udp_packets_dropped_total", "Total UDP telemetry packets dropped under backpressure", ""),
		UDPTelemetryDuration: NewHistogram("forest_udp_telemetry_duration_seconds", "Duration of processing UDP telemetry frames", nil),

		MemTableSwapEvents:   NewCounter("forest_memtable_swap_events_total", "Total MemTable rotation and swap events", ""),
		MemTableSwapDuration: NewHistogram("forest_memtable_swap_duration_seconds", "Duration of freezing and rotating active MemTable", nil),
		MemTableActiveBytes:  NewGauge("forest_memtable_active_bytes", "Estimated active MemTable memory usage in bytes"),
		WALFsyncTotal:        NewCounter("forest_wal_fsync_total", "Total WAL background fsync operations completed", ""),
		WALFsyncDuration:     NewHistogram("forest_wal_fsync_duration_seconds", "Latency distribution of WAL file fsync calls", nil),
	}

	reg.RegisterCounter(em.TCPRequestsPutOk)
	reg.RegisterCounter(em.TCPRequestsGetOk)
	reg.RegisterCounter(em.TCPRequestsDelOk)
	reg.RegisterCounter(em.TCPRequestsEchoOk)
	reg.RegisterCounter(em.TCPRequestsScanOk)
	reg.RegisterCounter(em.TCPRequestsErrors)
	reg.RegisterHistogram(em.TCPParseDuration)

	reg.RegisterCounter(em.UDPPacketsTotal)
	reg.RegisterCounter(em.UDPPacketsDropped)
	reg.RegisterHistogram(em.UDPTelemetryDuration)

	reg.RegisterCounter(em.MemTableSwapEvents)
	reg.RegisterHistogram(em.MemTableSwapDuration)
	reg.RegisterGauge(em.MemTableActiveBytes)
	reg.RegisterCounter(em.WALFsyncTotal)
	reg.RegisterHistogram(em.WALFsyncDuration)

	return em
}

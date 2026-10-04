// Package metrics owns the Prometheus registry and every metric the gateway
// exports. Keeping them in one place makes the /metrics surface reviewable.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry struct {
	Registry *prometheus.Registry

	// EventsReceived counts telemetry events received over gRPC, before any
	// validation or publishing.
	EventsReceived prometheus.Counter
	// EventsAccepted counts events in batches confirmed by Kafka.
	EventsAccepted prometheus.Counter
	// BatchesRejected counts requests that fail validation.
	BatchesRejected prometheus.Counter
	// KafkaPublicationErrors counts every non-context publisher error, including
	// local producer contract failures and Kafka delivery failures.
	KafkaPublicationErrors prometheus.Counter
	// BatchSize observes submitted request sizes, including invalid batches with events.
	BatchSize prometheus.Histogram
	// PublicationDuration measures the time waiting for a publisher result.
	PublicationDuration prometheus.Histogram

	// ActiveRequests tracks ExportBatch RPC handlers currently in flight.
	ActiveRequests prometheus.Gauge
}

func NewRegistry() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	r := &Registry{
		Registry: reg,
		EventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_gateway_events_received_total",
			Help: "Telemetry events received over gRPC (pre-validation).",
		}),
		EventsAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_gateway_events_accepted_total",
			Help: "Telemetry events in Kafka-acknowledged batches.",
		}),
		BatchesRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_gateway_batches_rejected_total",
			Help: "ExportBatch requests rejected by validation.",
		}),
		KafkaPublicationErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_gateway_kafka_publication_errors_total",
			Help: "Non-context Kafka publisher failures, including producer contract failures.",
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulse_gateway_batch_size_events",
			Help:    "Events per ExportBatch request with a nonempty event list, before validation.",
			Buckets: []float64{1, 10, 50, 100, 250, 500},
		}),
		PublicationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulse_gateway_publication_duration_seconds",
			Help:    "Time spent waiting for a Kafka batch publication result.",
			Buckets: prometheus.DefBuckets,
		}),
		ActiveRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulse_gateway_active_requests",
			Help: "ExportBatch RPC handlers currently in flight.",
		}),
	}
	reg.MustRegister(r.EventsReceived, r.EventsAccepted, r.BatchesRejected, r.KafkaPublicationErrors, r.BatchSize, r.PublicationDuration, r.ActiveRequests)
	return r
}

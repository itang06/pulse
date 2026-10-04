package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pulsev1 "github.com/itang06/pulse/gateway/gen/pulse/v1"
	"github.com/itang06/pulse/gateway/internal/kafka"
	"github.com/itang06/pulse/gateway/internal/metrics"
)

type fakePublisher struct {
	calls   int
	records []kafka.Record
	err     error
	publish func(context.Context, []kafka.Record) error
}

func (f *fakePublisher) PublishBatch(ctx context.Context, records []kafka.Record) error {
	f.calls++
	f.records = records
	if f.publish != nil {
		return f.publish(ctx, records)
	}
	return f.err
}

func validRequest() *pulsev1.ExportBatchRequest {
	return &pulsev1.ExportBatchRequest{
		BatchId: "123e4567-e89b-42d3-a456-426614174001",
		Events: []*pulsev1.TelemetryEvent{{
			EventId:     "123e4567-e89b-42d3-a456-426614174000",
			EventTime:   timestamppb.Now(),
			ServiceName: "checkout",
			Route:       "/orders",
		}},
	}
}

func TestExportBatchPublishesEveryValidatedEvent(t *testing.T) {
	registry := metrics.NewRegistry()
	publisher := &fakePublisher{}
	server := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, registry)

	response, err := server.ExportBatch(context.Background(), validRequest())

	if err != nil {
		t.Fatalf("ExportBatch() error = %v, want nil", err)
	}
	if response.GetBatchId() != "123e4567-e89b-42d3-a456-426614174001" || response.GetAcceptedCount() != 1 {
		t.Fatalf("ExportBatch() response = %v, want batch ID and one accepted event", response)
	}
	if publisher.calls != 1 || len(publisher.records) != 1 {
		t.Fatalf("PublishBatch calls = %d, records = %d; want one call with one record", publisher.calls, len(publisher.records))
	}
}

func TestExportBatchSerializesCompleteEventsWithDistinctOwnedRecords(t *testing.T) {
	publisher := &fakePublisher{}
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, metrics.NewRegistry())
	req := validRequest()
	req.Events[0].LatencyUs = 1042
	req.Events[0].StatusCode = 503
	req.Events[0].TraceId = "trace-0"
	req.Events[0].Attributes = map[string]string{"region": "west"}
	second := proto.Clone(req.Events[0]).(*pulsev1.TelemetryEvent)
	second.EventId = "123e4567-e89b-42d3-a456-426614174002"
	second.ServiceName = "billing"
	second.Route = "/invoice"
	req.Events = append(req.Events, second)
	want := []*pulsev1.TelemetryEvent{proto.Clone(req.Events[0]).(*pulsev1.TelemetryEvent), proto.Clone(second).(*pulsev1.TelemetryEvent)}

	response, err := s.ExportBatch(context.Background(), req)
	if err != nil || response.GetAcceptedCount() != 2 {
		t.Fatalf("ExportBatch() = (%v, %v), want two accepted events", response, err)
	}
	if publisher.calls != 1 || len(publisher.records) != 2 {
		t.Fatalf("PublishBatch calls = %d, records = %d; want one call with both records", publisher.calls, len(publisher.records))
	}
	for i, key := range []string{"checkout\x00/orders", "billing\x00/invoice"} {
		if string(publisher.records[i].Key) != key {
			t.Errorf("record %d key = %q, want %q", i, publisher.records[i].Key, key)
		}
		var decoded pulsev1.TelemetryEvent
		if err := proto.Unmarshal(publisher.records[i].Value, &decoded); err != nil {
			t.Fatalf("record %d has invalid protobuf: %v", i, err)
		}
		if !proto.Equal(&decoded, want[i]) {
			t.Errorf("record %d event = %v, want %v", i, &decoded, want[i])
		}
	}
	keyBefore := string(publisher.records[0].Key)
	valueBefore := append([]byte(nil), publisher.records[0].Value...)
	req.Events[0].ServiceName = "modified"
	req.Events[0].Attributes["region"] = "changed"
	if string(publisher.records[0].Key) != keyBefore || !reflect.DeepEqual(publisher.records[0].Value, valueBefore) {
		t.Fatal("published record buffers changed after request mutation")
	}
	for i := range publisher.records[0].Key {
		publisher.records[0].Key[i] = 'x'
	}
	if string(publisher.records[1].Key) != "billing\x00/invoice" {
		t.Fatal("record keys share a mutable buffer")
	}
}

func TestExportBatchRejectsLaterInvalidEventWithoutPublishing(t *testing.T) {
	publisher := &fakePublisher{}
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, metrics.NewRegistry())
	req := validRequest()
	req.Events = append(req.Events, &pulsev1.TelemetryEvent{EventId: "bad"})
	response, err := s.ExportBatch(context.Background(), req)
	if response != nil || status.Code(err) != codes.InvalidArgument || publisher.calls != 0 {
		t.Fatalf("ExportBatch() = (%v, %v), publish calls = %d; want invalid argument with no publish", response, err, publisher.calls)
	}
}

func TestExportBatchMapsPublicationErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"buffer full", kafka.ErrBufferFull, codes.ResourceExhausted},
		{"wrapped buffer full", errors.Join(errors.New("publish"), kafka.ErrBufferFull), codes.ResourceExhausted},
		{"delivery failure", &kafka.ErrDelivery{Cause: errors.New("broker down")}, codes.Unavailable},
		{"oversized record", kafka.ErrRecordTooLarge, codes.InvalidArgument},
		{"oversized record also permanent", errors.Join(kafka.ErrRecordTooLarge, kafka.ErrPermanentDelivery), codes.InvalidArgument},
		{"permanent delivery", kafka.ErrPermanentDelivery, codes.FailedPrecondition},
		{"generic failure", errors.New("broker down"), codes.Unavailable},
		{"empty batch programming error", kafka.ErrEmptyBatch, codes.Internal},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &fakePublisher{err: tt.err}
			s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, metrics.NewRegistry())
			response, err := s.ExportBatch(context.Background(), validRequest())
			if response != nil || status.Code(err) != tt.code || publisher.calls != 1 {
				t.Fatalf("ExportBatch() = (%v, %v), publish calls = %d; want %v and one call", response, err, publisher.calls, tt.code)
			}
		})
	}
}

func TestExportBatchStoppedContextNeverPublishes(t *testing.T) {
	for _, tt := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		code codes.Code
	}{
		{"canceled", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, codes.Canceled},
		{"deadline exceeded", func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}, codes.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()
			publisher := &fakePublisher{}
			s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, metrics.NewRegistry())
			response, err := s.ExportBatch(ctx, validRequest())
			if response != nil || status.Code(err) != tt.code || publisher.calls != 0 {
				t.Fatalf("ExportBatch() = (%v, %v), publish calls = %d; want %v without publish", response, err, publisher.calls, tt.code)
			}
		})
	}
}

func TestExportBatchMissingPublisherIsInternal(t *testing.T) {
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, metrics.NewRegistry())
	response, err := s.ExportBatch(context.Background(), validRequest())
	if response != nil || status.Code(err) != codes.Internal {
		t.Fatalf("ExportBatch() = (%v, %v), want Internal with no response", response, err)
	}
}

func TestExportBatchWaitsForPublisherCompletion(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	publisher := &fakePublisher{publish: func(_ context.Context, _ []kafka.Record) error {
		close(entered)
		<-release
		return nil
	}}
	reg := metrics.NewRegistry()
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, reg)
	done := make(chan error, 1)
	go func() {
		_, err := s.ExportBatch(context.Background(), validRequest())
		done <- err
	}()
	<-entered
	if got := metricGauge(t, reg.Registry, "pulse_gateway_active_requests"); got != 1 {
		t.Errorf("active requests while publishing = %v, want 1", got)
	}
	select {
	case err := <-done:
		t.Fatalf("ExportBatch returned before publisher completion: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ExportBatch() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExportBatch did not return after publisher completion")
	}
	if got := metricGauge(t, reg.Registry, "pulse_gateway_active_requests"); got != 0 {
		t.Errorf("active requests after return = %v, want 0", got)
	}
}

func TestExportBatchCountsAcknowledgedEventsWhenContextExpiresAfterPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := &fakePublisher{publish: func(_ context.Context, _ []kafka.Record) error {
		cancel()
		return nil
	}}
	reg := metrics.NewRegistry()
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, reg)
	response, err := s.ExportBatch(ctx, validRequest())
	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("ExportBatch() = (%v, %v), want Canceled without response", response, err)
	}
	if got := metricGauge(t, reg.Registry, "pulse_gateway_events_accepted_total"); got != 1 {
		t.Errorf("Kafka-acknowledged events = %v, want 1 despite canceled RPC", got)
	}
	if got := metricGauge(t, reg.Registry, "pulse_gateway_kafka_publication_errors_total"); got != 0 {
		t.Errorf("Kafka errors = %v, want 0 after successful publish", got)
	}
}

func TestExportBatchCountsPublisherErrorWhenContextExpiresAfterPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := &fakePublisher{publish: func(_ context.Context, _ []kafka.Record) error {
		cancel()
		return kafka.ErrBufferFull
	}}
	reg := metrics.NewRegistry()
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, reg)
	response, err := s.ExportBatch(ctx, validRequest())
	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("ExportBatch() = (%v, %v), want Canceled without response", response, err)
	}
	if got := metricGauge(t, reg.Registry, "pulse_gateway_kafka_publication_errors_total"); got != 1 {
		t.Errorf("Kafka publication errors = %v, want 1 despite canceled RPC", got)
	}
	if got := metricGauge(t, reg.Registry, "pulse_gateway_events_accepted_total"); got != 0 {
		t.Errorf("Kafka-acknowledged events = %v, want 0 after failed publish", got)
	}
}

func TestExportBatchMetricsCountOutcomes(t *testing.T) {
	reg := metrics.NewRegistry()
	publisher := &fakePublisher{}
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher, reg)
	if _, err := s.ExportBatch(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request status = %v", status.Code(err))
	}
	req := validRequest()
	req.Events = append(req.Events, nil)
	if _, err := s.ExportBatch(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid event status = %v", status.Code(err))
	}
	publisher.err = kafka.ErrBufferFull
	if _, err := s.ExportBatch(context.Background(), validRequest()); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("buffer failure status = %v", status.Code(err))
	}
	publisher.err = kafka.ErrEmptyBatch
	if _, err := s.ExportBatch(context.Background(), validRequest()); status.Code(err) != codes.Internal {
		t.Fatalf("publisher contract failure status = %v", status.Code(err))
	}
	publisher.err = nil
	if _, err := s.ExportBatch(context.Background(), validRequest()); err != nil {
		t.Fatalf("successful batch error = %v", err)
	}
	for name, want := range map[string]float64{
		"pulse_gateway_events_received_total":          5,
		"pulse_gateway_events_accepted_total":          1,
		"pulse_gateway_batches_rejected_total":         2,
		"pulse_gateway_kafka_publication_errors_total": 2,
		"pulse_gateway_active_requests":                0,
	} {
		if got := metricGauge(t, reg.Registry, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	for name, want := range map[string]uint64{
		"pulse_gateway_batch_size_events":            4,
		"pulse_gateway_publication_duration_seconds": 3,
	} {
		if got := histogramCount(t, reg.Registry, name); got != want {
			t.Errorf("%s samples = %d, want %d", name, got, want)
		}
	}
}

func metricGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			if len(family.Metric) != 1 {
				t.Fatalf("%s metric count = %d, want 1", name, len(family.Metric))
			}
			if c := family.Metric[0].Counter; c != nil {
				return c.GetValue()
			}
			if g := family.Metric[0].Gauge; g != nil {
				return g.GetValue()
			}
			t.Fatalf("%s is not a counter or gauge", name)
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			if len(family.Metric) != 1 || family.Metric[0].Histogram == nil {
				t.Fatalf("%s is not a single histogram", name)
			}
			return family.Metric[0].Histogram.GetSampleCount()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func TestExportBatchRejectsInvalidRequestBeforePublication(t *testing.T) {
	registry := metrics.NewRegistry()
	server := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, registry)

	response, err := server.ExportBatch(context.Background(), &pulsev1.ExportBatchRequest{})

	if response != nil {
		t.Fatalf("ExportBatch() response = %v, want nil", response)
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("ExportBatch() status = %v, want %v", got, codes.InvalidArgument)
	}
}

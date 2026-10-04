// Package server implements pulse.v1.TelemetryService.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pulsev1 "github.com/itang06/pulse/gateway/gen/pulse/v1"
	"github.com/itang06/pulse/gateway/internal/kafka"
	"github.com/itang06/pulse/gateway/internal/metrics"
)

type Server struct {
	pulsev1.UnimplementedTelemetryServiceServer

	log      *slog.Logger
	producer RawPublisher
	metrics  *metrics.Registry
}

type RawPublisher interface {
	PublishBatch(context.Context, []kafka.Record) error
}

func New(log *slog.Logger, producer RawPublisher, m *metrics.Registry) *Server {
	return &Server{log: log, producer: producer, metrics: m}
}

// ExportBatch acknowledges a batch only after the publisher confirms every record.
func (s *Server) ExportBatch(
	ctx context.Context,
	req *pulsev1.ExportBatchRequest,
) (*pulsev1.ExportBatchResponse, error) {
	s.metrics.ActiveRequests.Inc()
	defer s.metrics.ActiveRequests.Dec()
	// Count what arrived, including events in a batch later rejected by validation.
	if req != nil {
		s.metrics.EventsReceived.Add(float64(len(req.GetEvents())))
		if len(req.GetEvents()) > 0 {
			s.metrics.BatchSize.Observe(float64(len(req.GetEvents())))
		}
	}

	if err := validateBatch(req, time.Now); err != nil {
		s.metrics.BatchesRejected.Inc()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.producer == nil {
		return nil, status.Error(codes.Internal, "Kafka publisher is not configured")
	}

	records := make([]kafka.Record, len(req.GetEvents()))
	for i, event := range req.GetEvents() {
		value, err := proto.Marshal(event)
		if err != nil {
			return nil, status.Error(codes.Internal, "serializing telemetry event")
		}
		records[i] = kafka.Record{
			Key:   []byte(event.GetServiceName() + "\x00" + event.GetRoute()),
			Value: value,
		}
	}
	start := time.Now()
	err := s.producer.PublishBatch(ctx, records)
	s.metrics.PublicationDuration.Observe(time.Since(start).Seconds())
	if err == nil {
		// The Kafka acknowledgement stands even if the RPC context expires now.
		s.metrics.EventsAccepted.Add(float64(len(records)))
	} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		// Record the publisher outcome even if RPC cancellation wins the status race.
		s.metrics.KafkaPublicationErrors.Inc()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		switch {
		case errors.Is(err, kafka.ErrEmptyBatch):
			return nil, status.Error(codes.Internal, "publisher received an empty batch")
		case errors.Is(err, kafka.ErrBufferFull):
			return nil, status.Error(codes.ResourceExhausted, "Kafka producer buffer is full")
		case errors.Is(err, kafka.ErrRecordTooLarge):
			return nil, status.Error(codes.InvalidArgument, "telemetry record exceeds Kafka's size limit")
		case errors.Is(err, kafka.ErrPermanentDelivery):
			return nil, status.Error(codes.FailedPrecondition, "Kafka permanently rejected the telemetry batch")
		default:
			return nil, status.Error(codes.Unavailable, "publishing telemetry batch")
		}
	}
	return &pulsev1.ExportBatchResponse{BatchId: req.GetBatchId(), AcceptedCount: uint64(len(records))}, nil
}

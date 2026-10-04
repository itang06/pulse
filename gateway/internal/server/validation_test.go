package server

import (
	"strings"
	"testing"
	"time"

	pulsev1 "github.com/itang06/pulse/gateway/gen/pulse/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateBatch(t *testing.T) {
	now := time.Date(2026, time.October, 4, 12, 0, 0, 0, time.UTC)
	validEvent := func() *pulsev1.TelemetryEvent {
		return &pulsev1.TelemetryEvent{
			EventId:     "123e4567-e89b-42d3-a456-426614174000",
			EventTime:   timestamppb.New(now),
			ServiceName: "checkout",
			Route:       "/orders",
			Attributes:  map[string]string{},
		}
	}
	validRequest := func() *pulsev1.ExportBatchRequest {
		return &pulsev1.ExportBatchRequest{
			BatchId: "123e4567-e89b-42d3-a456-426614174001",
			Events:  []*pulsev1.TelemetryEvent{validEvent()},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest
		wantErr string
	}{
		{name: "valid"},
		{name: "exact event limit is valid", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events = make([]*pulsev1.TelemetryEvent, 500)
			for i := range r.Events {
				r.Events[i] = validEvent()
			}
			return r
		}},
		{name: "exact route byte limit is valid", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Route = strings.Repeat("a", 256)
			return r
		}},
		{name: "exact attribute limits are valid", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			for i := 0; i < 16; i++ {
				r.Events[0].Attributes[strings.Repeat("k", i+1)] = strings.Repeat("v", 256)
			}
			return r
		}},
		{name: "nil request", mutate: func(*pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { return nil }, wantErr: "request"},
		{name: "empty batch id", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.BatchId = ""; return r }, wantErr: "batch_id"},
		{name: "malformed batch id", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.BatchId = "batch-1"; return r }, wantErr: "batch_id"},
		{name: "empty event list", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.Events = nil; return r }, wantErr: "at least 1"},
		{name: "too many events", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events = make([]*pulsev1.TelemetryEvent, 501)
			return r
		}, wantErr: "at most 500"},
		{name: "nil event", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.Events[0] = nil; return r }, wantErr: "events[0]"},
		{name: "malformed event id", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventId = "not-a-uuid"
			return r
		}, wantErr: "events[0].event_id"},
		{name: "later event error identifies its index", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events = append(r.Events, validEvent())
			r.Events[1].EventId = "not-a-uuid"
			return r
		}, wantErr: "events[1].event_id"},
		{name: "missing timestamp", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.Events[0].EventTime = nil; return r }, wantErr: "events[0].event_time"},
		{name: "invalid protobuf timestamp", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventTime = &timestamppb.Timestamp{Seconds: 253402300800}
			return r
		}, wantErr: "events[0].event_time"},
		{name: "timestamp too old", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventTime = timestamppb.New(now.Add(-24*time.Hour - time.Nanosecond))
			return r
		}, wantErr: "24 hours"},
		{name: "timestamp too far ahead", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventTime = timestamppb.New(now.Add(5*time.Minute + time.Nanosecond))
			return r
		}, wantErr: "5 minutes"},
		{name: "exact oldest timestamp is valid", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventTime = timestamppb.New(now.Add(-24 * time.Hour))
			return r
		}},
		{name: "exact latest timestamp is valid", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].EventTime = timestamppb.New(now.Add(5 * time.Minute))
			return r
		}},
		{name: "empty service name", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].ServiceName = ""
			return r
		}, wantErr: "events[0].service_name"},
		{name: "empty route", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest { r.Events[0].Route = ""; return r }, wantErr: "events[0].route"},
		{name: "route over byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Route = strings.Repeat("a", 257)
			return r
		}, wantErr: "256 bytes"},
		{name: "unicode route uses byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Route = strings.Repeat("界", 86)
			return r
		}, wantErr: "256 bytes"},
		{name: "NUL in service name", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].ServiceName = "checkout\x00api"
			return r
		}, wantErr: "NUL"},
		{name: "NUL in route", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Route = "/orders\x00private"
			return r
		}, wantErr: "NUL"},
		{name: "too many attributes", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			for i := 0; i < 17; i++ {
				r.Events[0].Attributes[strings.Repeat("k", i+1)] = "v"
			}
			return r
		}, wantErr: "16 attributes"},
		{name: "attribute key over byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Attributes[strings.Repeat("k", 65)] = "v"
			return r
		}, wantErr: "64 bytes"},
		{name: "unicode attribute key uses byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Attributes[strings.Repeat("界", 22)] = "v"
			return r
		}, wantErr: "64 bytes"},
		{name: "attribute value over byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Attributes["key"] = strings.Repeat("v", 257)
			return r
		}, wantErr: "256 bytes"},
		{name: "unicode attribute value uses byte limit", mutate: func(r *pulsev1.ExportBatchRequest) *pulsev1.ExportBatchRequest {
			r.Events[0].Attributes["key"] = strings.Repeat("界", 86)
			return r
		}, wantErr: "256 bytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			if tt.mutate != nil {
				req = tt.mutate(req)
			}
			err := validateBatch(req, func() time.Time { return now })
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateBatch() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateBatch() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

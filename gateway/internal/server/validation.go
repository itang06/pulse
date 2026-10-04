package server

import (
	"fmt"
	"sort"
	"strings"
	"time"

	pulsev1 "github.com/itang06/pulse/gateway/gen/pulse/v1"
)

const (
	maxBatchEvents       = 500
	maxEventAge          = 24 * time.Hour
	maxFutureEventTime   = 5 * time.Minute
	maxRouteBytes        = 256
	maxAttributeCount    = 16
	maxAttributeKeyBytes = 64
	maxAttributeValBytes = 256
)

func validateBatch(req *pulsev1.ExportBatchRequest, now func() time.Time) error {
	if req == nil {
		return fmt.Errorf("request is required")
	}
	if !isUUID(req.GetBatchId()) {
		return fmt.Errorf("batch_id must be a valid UUID")
	}
	if len(req.GetEvents()) == 0 {
		return fmt.Errorf("events must contain at least 1 event")
	}
	if len(req.GetEvents()) > maxBatchEvents {
		return fmt.Errorf("events must contain at most %d events", maxBatchEvents)
	}

	currentTime := now()
	oldestAllowed := currentTime.Add(-maxEventAge)
	newestAllowed := currentTime.Add(maxFutureEventTime)
	for i, event := range req.GetEvents() {
		if event == nil {
			return fmt.Errorf("events[%d] is required", i)
		}
		if !isUUID(event.GetEventId()) {
			return fmt.Errorf("events[%d].event_id must be a valid UUID", i)
		}
		if event.GetEventTime() == nil {
			return fmt.Errorf("events[%d].event_time is required", i)
		}
		if err := event.GetEventTime().CheckValid(); err != nil {
			return fmt.Errorf("events[%d].event_time is invalid: %w", i, err)
		}
		eventTime := event.GetEventTime().AsTime()
		if eventTime.Before(oldestAllowed) {
			return fmt.Errorf("events[%d].event_time must not be older than 24 hours", i)
		}
		if eventTime.After(newestAllowed) {
			return fmt.Errorf("events[%d].event_time must not be more than 5 minutes in the future", i)
		}
		if event.GetServiceName() == "" {
			return fmt.Errorf("events[%d].service_name is required", i)
		}
		if strings.IndexByte(event.GetServiceName(), 0) >= 0 {
			return fmt.Errorf("events[%d].service_name must not contain NUL", i)
		}
		if event.GetRoute() == "" {
			return fmt.Errorf("events[%d].route is required", i)
		}
		if strings.IndexByte(event.GetRoute(), 0) >= 0 {
			return fmt.Errorf("events[%d].route must not contain NUL", i)
		}
		if len(event.GetRoute()) > maxRouteBytes {
			return fmt.Errorf("events[%d].route must be at most %d bytes", i, maxRouteBytes)
		}
		if len(event.GetAttributes()) > maxAttributeCount {
			return fmt.Errorf("events[%d].attributes must contain at most %d attributes", i, maxAttributeCount)
		}
		keys := make([]string, 0, len(event.GetAttributes()))
		for key := range event.GetAttributes() {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if len(key) > maxAttributeKeyBytes {
				return fmt.Errorf("events[%d].attributes[%q] key must be at most %d bytes", i, key, maxAttributeKeyBytes)
			}
			if len(event.GetAttributes()[key]) > maxAttributeValBytes {
				return fmt.Errorf("events[%d].attributes value for key %q must be at most %d bytes", i, key, maxAttributeValBytes)
			}
		}
	}
	return nil
}

// isUUID accepts the canonical 8-4-4-4-12 hexadecimal UUID representation.
// It intentionally checks syntax only so nil and future-version UUID values
// remain valid identifiers.
func isUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := value[i]
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'f') && !('A' <= c && c <= 'F') {
			return false
		}
	}
	return true
}

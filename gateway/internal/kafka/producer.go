// Package kafka wraps the franz-go client behind the small surface the
// gateway actually needs, so the rest of the code never imports kgo directly.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic names are created by deploy/docker-compose.yml (kafka-init service);
// auto-creation is deliberately disabled so topic config stays explicit.
const (
	TopicRaw = "telemetry.raw"
	// The gateway accepts at most 500 events per RPC. Eight concurrent full
	// batches fit in this buffer before local backpressure rejects more work.
	maxBufferedRecords = 4000
	// Cap aggregate payload memory independently of record count. franz-go
	// rejects a single record above this limit with MessageTooLarge.
	maxBufferedBytes = 64 << 20
)

// ErrBufferFull means the local producer has no room for another record.
var ErrBufferFull = errors.New("kafka producer buffer full")

// ErrRecordTooLarge means Kafka permanently rejected a record's size.
var ErrRecordTooLarge = errors.New("kafka record too large")

// ErrPermanentDelivery means Kafka rejected a record with a non-retriable error.
var ErrPermanentDelivery = errors.New("kafka permanent delivery failure")

// ErrEmptyBatch means there were no records to publish or acknowledge.
var ErrEmptyBatch = errors.New("kafka batch is empty")

// ErrDelivery wraps a failed Kafka delivery callback.
type ErrDelivery struct{ Cause error }

func (e *ErrDelivery) Error() string { return fmt.Sprintf("kafka delivery failed: %v", e.Cause) }
func (e *ErrDelivery) Unwrap() error { return e.Cause }

// Record is a serialized telemetry event and its Kafka partition key.
// PublishBatch takes ownership of Key and Value until their delivery callbacks
// finish. Callers must not mutate or reuse these buffers, even after a
// cancellation return, because franz-go may still be delivering records.
type Record struct {
	Key   []byte
	Value []byte
}

// recordProducer is intentionally small so delivery timing and failures can
// be exercised without a live broker. TryProduce reports errors in its
// callback; the franz-go method has no error return.
type recordProducer interface {
	TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Ping(context.Context) error
	Close()
}

type Producer struct {
	client recordProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.MaxBufferedRecords(maxBufferedRecords),
		kgo.MaxBufferedBytes(maxBufferedBytes),
		// Keep franz-go's default idempotent production: disabling it would
		// weaken deduplication of retries inside one producer session.
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

// Ping verifies the cluster is reachable.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// PublishBatch returns success only after every record's Kafka delivery
// callback succeeds. A rejected record fails the whole batch; the caller
// retries all events with their original IDs, so earlier publications may
// appear again and downstream consumers must deduplicate by event ID.
func (p *Producer) PublishBatch(ctx context.Context, records []Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) == 0 {
		return ErrEmptyBatch
	}
	batch := &batchResult{done: make(chan struct{})}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			batch.seal()
			return err
		}
		batch.add()
		var once sync.Once
		p.client.TryProduce(ctx, &kgo.Record{
			Topic: TopicRaw,
			Key:   record.Key,
			Value: record.Value,
		}, func(_ *kgo.Record, err error) {
			once.Do(func() { batch.complete(err) })
		})
		// The callback for a pre-buffer rejection can be synchronous. Stop
		// submitting as soon as that is observable. Asynchronous callbacks
		// can arrive after subsequent TryProduce calls have been submitted.
		if batch.failed() {
			break
		}
	}
	batch.seal()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return batch.result()
	}
}

type batchResult struct {
	mu      sync.Mutex
	pending int
	sealed  bool
	err     error
	done    chan struct{}
}

func (b *batchResult) add() {
	b.mu.Lock()
	b.pending++
	b.mu.Unlock()
}

func (b *batchResult) complete(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		var classified error
		var kafkaErr *kerr.Error
		switch {
		case errors.Is(err, kerr.MessageTooLarge):
			classified = fmt.Errorf("%w: %w: %w", ErrRecordTooLarge, ErrPermanentDelivery, err)
		case errors.As(err, &kafkaErr) && !kerr.IsRetriable(err):
			classified = fmt.Errorf("%w: %w", ErrPermanentDelivery, err)
		case errors.Is(err, kgo.ErrMaxBuffered):
			classified = ErrBufferFull
		default:
			classified = &ErrDelivery{Cause: err}
		}
		// Keep the strongest failure independent of callback completion order.
		if b.err == nil || deliveryPriority(classified) > deliveryPriority(b.err) {
			b.err = classified
		}
	}
	b.pending--
	if b.sealed && b.pending == 0 {
		close(b.done)
	}
}

func deliveryPriority(err error) int {
	switch {
	case errors.Is(err, ErrRecordTooLarge):
		return 3
	case errors.Is(err, ErrPermanentDelivery):
		return 2
	default:
		return 1
	}
}

func (b *batchResult) failed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err != nil
}

func (b *batchResult) seal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed = true
	if b.pending == 0 {
		close(b.done)
	}
}

func (b *batchResult) result() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (p *Producer) Close() {
	p.client.Close()
}

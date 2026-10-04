package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type pendingRecord struct {
	record  *kgo.Record
	deliver func(*kgo.Record, error)
}

type fakeClient struct {
	produce func(context.Context, *kgo.Record, func(*kgo.Record, error))
}

func (f *fakeClient) TryProduce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	f.produce(ctx, record, callback)
}

func (*fakeClient) Ping(context.Context) error { return nil }
func (*fakeClient) Close()                     {}

func testRecords() []Record {
	return []Record{
		{Key: []byte("svc:a"), Value: []byte("one")},
		{Key: []byte("svc:b"), Value: []byte("two")},
		{Key: []byte("svc:c"), Value: []byte("three")},
	}
}

func awaitPending(t *testing.T, pending <-chan pendingRecord) pendingRecord {
	t.Helper()
	select {
	case item := <-pending:
		return item
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for submitted record")
		return pendingRecord{}
	}
}

func awaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PublishBatch")
		return nil
	}
}

func assertStillWaiting(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("PublishBatch returned before all callbacks: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestPublishBatchRejectsEmptyBatch(t *testing.T) {
	p := &Producer{client: &fakeClient{produce: func(context.Context, *kgo.Record, func(*kgo.Record, error)) {
		t.Fatal("empty batch must not submit records")
	}}}
	if err := p.PublishBatch(context.Background(), nil); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("got %v, want ErrEmptyBatch", err)
	}
}

func TestNewProducerRejectsRecordLargerThanByteBuffer(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = p.PublishBatch(ctx, []Record{{Value: make([]byte, 64<<20+1)}})
	if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("got %v, want ErrRecordTooLarge preserving MessageTooLarge", err)
	}
}

func TestPublishBatchClassifiesOversizedDelivery(t *testing.T) {
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		cb(r, errors.Join(errors.New("produce rejected"), kerr.MessageTooLarge))
	}}}
	err := p.PublishBatch(context.Background(), testRecords()[:1])
	if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("got %v, want permanent oversized classification preserving Kafka cause", err)
	}
}

func TestPublishBatchReturnsAfterEveryDeliveryCallback(t *testing.T) {
	pending := make(chan pendingRecord, 3)
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		pending <- pendingRecord{r, cb}
	}}}
	result := make(chan error, 1)
	go func() { result <- p.PublishBatch(context.Background(), testRecords()) }()
	items := []pendingRecord{awaitPending(t, pending), awaitPending(t, pending), awaitPending(t, pending)}
	for i, item := range items {
		if item.record.Topic != TopicRaw || string(item.record.Key) != string(testRecords()[i].Key) || string(item.record.Value) != string(testRecords()[i].Value) {
			t.Fatalf("record %d not mapped to telemetry.raw: %+v", i, item.record)
		}
	}
	items[0].deliver(items[0].record, nil)
	items[1].deliver(items[1].record, nil)
	assertStillWaiting(t, result)
	items[2].deliver(items[2].record, nil)
	if err := awaitResult(t, result); err != nil {
		t.Fatalf("PublishBatch failed after all deliveries: %v", err)
	}
}

func TestPublishBatchReturnsBufferFullWithoutWaiting(t *testing.T) {
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		cb(r, kgo.ErrMaxBuffered)
	}}}
	if err := p.PublishBatch(context.Background(), testRecords()); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("got %v, want ErrBufferFull", err)
	}
}

func TestPublishBatchDrainsEarlierCallbacksAfterBufferFull(t *testing.T) {
	pending := make(chan pendingRecord, 1)
	var submitted int
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		submitted++
		if submitted == 1 {
			pending <- pendingRecord{r, cb}
			return
		}
		cb(r, kgo.ErrMaxBuffered)
	}}}
	result := make(chan error, 1)
	go func() { result <- p.PublishBatch(context.Background(), testRecords()) }()
	first := awaitPending(t, pending)
	assertStillWaiting(t, result)
	first.deliver(first.record, nil)
	if err := awaitResult(t, result); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("got %v, want ErrBufferFull", err)
	}
	if submitted != 2 {
		t.Fatalf("submitted %d records after synchronous rejection, want 2", submitted)
	}
}

func TestPublishBatchReturnsAnyDeliveryFailure(t *testing.T) {
	pending := make(chan pendingRecord, 3)
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		pending <- pendingRecord{r, cb}
	}}}
	result := make(chan error, 1)
	go func() { result <- p.PublishBatch(context.Background(), testRecords()) }()
	items := []pendingRecord{awaitPending(t, pending), awaitPending(t, pending), awaitPending(t, pending)}
	cause := errors.New("broker unavailable")
	items[0].deliver(items[0].record, cause)
	items[1].deliver(items[1].record, nil)
	assertStillWaiting(t, result)
	items[2].deliver(items[2].record, nil)
	err := awaitResult(t, result)
	var deliveryErr *ErrDelivery
	if !errors.As(err, &deliveryErr) || !errors.Is(err, cause) {
		t.Fatalf("got %v, want wrapped ErrDelivery and original cause", err)
	}
}

func TestPublishBatchPermanentOversizeOutranksOtherCallbackErrors(t *testing.T) {
	for _, tt := range []struct {
		name   string
		first  error
		second error
	}{
		{"generic then oversized", errors.New("broker unavailable"), kerr.MessageTooLarge},
		{"oversized then generic", kerr.MessageTooLarge, errors.New("broker unavailable")},
		{"buffer full then oversized", kgo.ErrMaxBuffered, kerr.MessageTooLarge},
		{"oversized then buffer full", kerr.MessageTooLarge, kgo.ErrMaxBuffered},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pending := make(chan pendingRecord, 2)
			p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
				pending <- pendingRecord{r, cb}
			}}}
			result := make(chan error, 1)
			go func() { result <- p.PublishBatch(context.Background(), testRecords()[:2]) }()
			first := awaitPending(t, pending)
			second := awaitPending(t, pending)
			first.deliver(first.record, tt.first)
			assertStillWaiting(t, result)
			second.deliver(second.record, tt.second)
			err := awaitResult(t, result)
			if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, kerr.MessageTooLarge) {
				t.Fatalf("got %v, want permanent oversized classification preserving Kafka cause", err)
			}
		})
	}
}

func TestPublishBatchPermanentKafkaErrorOutranksRetryableCallbacks(t *testing.T) {
	for _, tt := range []struct {
		name      string
		first     error
		second    error
		wantCause error
	}{
		{"retryable then invalid record", kerr.NotLeaderForPartition, kerr.InvalidRecord, kerr.InvalidRecord},
		{"invalid record then retryable", kerr.InvalidRecord, kerr.NotLeaderForPartition, kerr.InvalidRecord},
		{"retryable then record list too large", kerr.RequestTimedOut, kerr.RecordListTooLarge, kerr.RecordListTooLarge},
		{"record list too large then retryable", kerr.RecordListTooLarge, kerr.RequestTimedOut, kerr.RecordListTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pending := make(chan pendingRecord, 2)
			p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
				pending <- pendingRecord{r, cb}
			}}}
			result := make(chan error, 1)
			go func() { result <- p.PublishBatch(context.Background(), testRecords()[:2]) }()
			first := awaitPending(t, pending)
			second := awaitPending(t, pending)
			first.deliver(first.record, tt.first)
			assertStillWaiting(t, result)
			second.deliver(second.record, tt.second)
			err := awaitResult(t, result)
			if !errors.Is(err, ErrPermanentDelivery) || !errors.Is(err, tt.wantCause) {
				t.Fatalf("got %v, want permanent Kafka classification preserving %v", err, tt.wantCause)
			}
			if errors.Is(err, ErrRecordTooLarge) {
				t.Fatalf("got %v, want generic permanent classification", err)
			}
		})
	}
}

func TestPublishBatchOversizeOutranksOtherPermanentKafkaError(t *testing.T) {
	for _, first := range []error{kerr.InvalidRecord, kerr.MessageTooLarge} {
		pending := make(chan pendingRecord, 2)
		p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
			pending <- pendingRecord{r, cb}
		}}}
		result := make(chan error, 1)
		go func() { result <- p.PublishBatch(context.Background(), testRecords()[:2]) }()
		items := []pendingRecord{awaitPending(t, pending), awaitPending(t, pending)}
		items[0].deliver(items[0].record, first)
		if first == kerr.InvalidRecord {
			items[1].deliver(items[1].record, kerr.MessageTooLarge)
		} else {
			items[1].deliver(items[1].record, kerr.InvalidRecord)
		}
		err := awaitResult(t, result)
		if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, kerr.MessageTooLarge) {
			t.Fatalf("got %v, want oversized classification to outrank InvalidRecord", err)
		}
	}
}

func TestPublishBatchHandlesCallbacksOutOfOrder(t *testing.T) {
	pending := make(chan pendingRecord, 3)
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		pending <- pendingRecord{r, cb}
	}}}
	result := make(chan error, 1)
	go func() { result <- p.PublishBatch(context.Background(), testRecords()) }()
	items := []pendingRecord{awaitPending(t, pending), awaitPending(t, pending), awaitPending(t, pending)}
	items[2].deliver(items[2].record, nil)
	items[0].deliver(items[0].record, nil)
	assertStillWaiting(t, result)
	items[1].deliver(items[1].record, nil)
	if err := awaitResult(t, result); err != nil {
		t.Fatalf("PublishBatch failed after out-of-order callbacks: %v", err)
	}
}

func TestPublishBatchHandlesSynchronousCallbacks(t *testing.T) {
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		cb(r, nil)
	}}}
	if err := p.PublishBatch(context.Background(), testRecords()); err != nil {
		t.Fatalf("PublishBatch failed with synchronous callbacks: %v", err)
	}
}

func TestPublishBatchHonorsContextCancellationAndLateCallbacks(t *testing.T) {
	pending := make(chan pendingRecord, 3)
	p := &Producer{client: &fakeClient{produce: func(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
		pending <- pendingRecord{r, cb}
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.PublishBatch(ctx, testRecords()) }()
	items := []pendingRecord{awaitPending(t, pending), awaitPending(t, pending), awaitPending(t, pending)}
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context canceled", err)
	}
	var callbacks sync.WaitGroup
	for _, item := range items {
		callbacks.Add(1)
		go func(item pendingRecord) {
			defer callbacks.Done()
			item.deliver(item.record, nil)
		}(item)
	}
	callbacks.Wait()
}

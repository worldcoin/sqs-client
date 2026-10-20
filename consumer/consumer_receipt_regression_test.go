package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

type syntheticReceiptClient struct {
	clock          atomic.Int64
	mu             sync.Mutex
	receivedAt     map[string]int64
	receives       int
	ages           []time.Duration
	expired        int
	secondReceived chan struct{}
	cancel         context.CancelFunc
	deleteErr      error
}

func (s *syntheticReceiptClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	s.mu.Lock()
	s.receives++
	n := s.receives
	if n > 2 {
		s.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	handle := "first"
	if n == 2 {
		handle = "second"
	}
	s.receivedAt[handle] = s.clock.Load()
	s.mu.Unlock()
	if n == 2 {
		close(s.secondReceived)
	}
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{{ReceiptHandle: aws.String(handle)}}}, nil
}

func (s *syntheticReceiptClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	age := time.Duration(s.clock.Load() - s.receivedAt[*in.ReceiptHandle])
	s.ages = append(s.ages, age)
	if len(s.ages) == 2 {
		s.cancel()
	}
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	if age >= 30*time.Second {
		s.expired++
		return nil, errors.New("synthetic receipt expired")
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (s *syntheticReceiptClient) Run(_ context.Context, m *Message) error {
	if *m.ReceiptHandle == "first" {
		// Let the old intake loop receive ahead while this worker remains occupied.
		// Correct intake waits for this handler and acknowledgment instead.
		select {
		case <-s.secondReceived:
		case <-time.After(20 * time.Millisecond):
		}
	}
	s.clock.Add(int64(16 * time.Second))
	return nil
}
func (*syntheticReceiptClient) Shutdown() {}

func TestReceiptBudgetTwoSixteenSecondHandlersDoNotExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &syntheticReceiptClient{receivedAt: make(map[string]int64), secondReceived: make(chan struct{}), cancel: cancel}
	c, err := NewConsumerWithSQSClient(s, Config{QueueURL: "test", WorkersNum: 1, BatchSize: 1, VisibilityTimeoutSeconds: 30}, s)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { c.Consume(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not finish")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, 0, s.expired, "prefetch makes the second receipt expire after waiting plus processing")
	require.Equal(t, []time.Duration{16 * time.Second, 16 * time.Second}, s.ages)
}

func TestReceiptBudgetRejectsInvalidConfiguration(t *testing.T) {
	zero := time.Duration(0)
	negative := -time.Second
	for _, tt := range []struct {
		name    string
		workers int
		batch   int32
		timeout *time.Duration
	}{
		{name: "zero workers", batch: 1},
		{name: "negative workers", workers: -1, batch: 1},
		{name: "zero batch", workers: 1},
		{name: "negative batch", workers: 1, batch: -1},
		{name: "oversized batch", workers: 1, batch: 11},
		{name: "zero timeout", workers: 1, batch: 1, timeout: &zero},
		{name: "negative timeout", workers: 1, batch: 1, timeout: &negative},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumerWithSQSClient(nil, Config{WorkersNum: tt.workers, BatchSize: tt.batch, VisibilityTimeoutSeconds: 30, HandlerTimeoutDuration: tt.timeout}, nil)
			require.Error(t, err)
			require.Nil(t, c)
		})
	}
}

func TestReceiptBudgetDeleteCancellationIsPropagated(t *testing.T) {
	s := &syntheticReceiptClient{receivedAt: map[string]int64{"first": 0}, deleteErr: context.Canceled}
	c, err := NewConsumerWithSQSClient(s, Config{WorkersNum: 1, BatchSize: 1, VisibilityTimeoutSeconds: 30}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, c.delete(context.Background(), newMessage(&types.Message{ReceiptHandle: aws.String("first")})), context.Canceled)
}

type canceledBatchClient struct {
	cancel   context.CancelFunc
	receives atomic.Int32
}

func (s *canceledBatchClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	s.receives.Add(1)
	s.cancel()
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{{ReceiptHandle: aws.String("first")}, {ReceiptHandle: aws.String("second")}}}, nil
}
func (*canceledBatchClient) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}
func (*canceledBatchClient) Run(context.Context, *Message) error { return nil }
func (*canceledBatchClient) Shutdown()                           {}

func TestReceiptBudgetCanceledBatchDispatchDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &canceledBatchClient{cancel: cancel}
	c, err := NewConsumerWithSQSClient(s, Config{WorkersNum: 2, BatchSize: 2, VisibilityTimeoutSeconds: 30}, s)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { c.Consume(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled batch dispatch did not release reservations and drain")
	}
	require.EqualValues(t, 1, s.receives.Load())
}

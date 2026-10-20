package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type receiptTestClient struct {
	receive func(context.Context, *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	delete  func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}

func (c receiptTestClient) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return c.receive(ctx, in)
}
func (c receiptTestClient) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return c.delete(ctx, in)
}

type receiptTestHandler struct {
	run      func(context.Context, *Message) error
	shutdown func()
}

func (h receiptTestHandler) Run(ctx context.Context, m *Message) error { return h.run(ctx, m) }
func (h receiptTestHandler) Shutdown() {
	if h.shutdown != nil {
		h.shutdown()
	}
}
func receiptConfig() Config {
	return Config{QueueURL: "test", WorkersNum: 1, BatchSize: 1, VisibilityTimeoutSeconds: 30}
}
func receiptOutput() *sqs.ReceiveMessageOutput {
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{{MessageId: aws.String("test"), ReceiptHandle: aws.String("test")}}}
}
func waitReceiptTest(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumer")
	}
}

func TestReceiptBudgetDoesNotPrefetchWhileWorkerBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var receives atomic.Int32
	client := receiptTestClient{receive: func(ctx context.Context, in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		receives.Add(1)
		require.EqualValues(t, 1, in.MaxNumberOfMessages)
		return receiptOutput(), nil
	}, delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		return &sqs.DeleteMessageOutput{}, nil
	}}
	handler := receiptTestHandler{run: func(context.Context, *Message) error { close(started); <-release; return nil }}
	c, err := NewConsumerWithSQSClient(client, receiptConfig(), handler)
	require.NoError(t, err)
	go func() { c.Consume(ctx); close(done) }()
	waitReceiptTest(t, started)
	time.Sleep(20 * time.Millisecond)
	require.EqualValues(t, 1, receives.Load(), "must reserve capacity before receiving another receipt")
	cancel()
	close(release)
	waitReceiptTest(t, done)
	require.EqualValues(t, 1, receives.Load())
}

func TestReceiptBudgetReleasesUnusedAndFailedReservations(t *testing.T) {
	for _, first := range []string{"short", "empty", "error"} {
		t.Run(first, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var calls atomic.Int32
			cfg := receiptConfig()
			cfg.WorkersNum = 2
			cfg.BatchSize = 10
			client := receiptTestClient{receive: func(ctx context.Context, in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				n := calls.Add(1)
				if n == 1 {
					require.EqualValues(t, 2, in.MaxNumberOfMessages)
					switch first {
					case "short":
						return receiptOutput(), nil
					case "empty":
						return &sqs.ReceiveMessageOutput{}, nil
					default:
						return nil, errors.New("test receive failure")
					}
				}
				if first == "short" {
					require.EqualValues(t, 1, in.MaxNumberOfMessages)
				} else {
					require.EqualValues(t, 2, in.MaxNumberOfMessages)
				}
				cancel()
				return nil, ctx.Err()
			}, delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
				return &sqs.DeleteMessageOutput{}, nil
			}}
			handler := receiptTestHandler{run: func(context.Context, *Message) error { <-ctx.Done(); return nil }}
			c, err := NewConsumerWithSQSClient(client, cfg, handler)
			require.NoError(t, err)
			go func() { c.Consume(ctx); close(done) }()
			waitReceiptTest(t, done)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestReceiptBudgetLateSuccessIsNotAcknowledged(t *testing.T) {
	timeout := time.Millisecond
	cfg := receiptConfig()
	cfg.HandlerTimeoutDuration = &timeout
	var deleted bool
	client := receiptTestClient{delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		deleted = true
		return &sqs.DeleteMessageOutput{}, nil
	}}
	handler := receiptTestHandler{run: func(ctx context.Context, _ *Message) error { <-ctx.Done(); return nil }}
	c, err := NewConsumerWithSQSClient(client, cfg, handler)
	require.NoError(t, err)
	err = c.handleMsg(context.Background(), newMessage(&receiptOutput().Messages[0]), time.Now().Add(30*time.Second))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, deleted)
}

func TestReceiptBudgetAcknowledgmentHasSeparateDeadline(t *testing.T) {
	timeout := 10 * time.Millisecond
	cfg := receiptConfig()
	cfg.HandlerTimeoutDuration = &timeout
	receiptDeadline := time.Now().Add(30 * time.Second)
	var handlerDeadline time.Time
	var deleted bool
	client := receiptTestClient{delete: func(ctx context.Context, _ *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, receiptDeadline, deadline)
		<-time.After(time.Until(handlerDeadline) + time.Millisecond)
		require.NoError(t, ctx.Err())
		deleted = true
		return &sqs.DeleteMessageOutput{}, nil
	}}
	handler := receiptTestHandler{run: func(ctx context.Context, _ *Message) error { handlerDeadline, _ = ctx.Deadline(); return nil }}
	c, err := NewConsumerWithSQSClient(client, cfg, handler)
	require.NoError(t, err)
	require.NoError(t, c.handleMsg(context.Background(), newMessage(&receiptOutput().Messages[0]), receiptDeadline))
	require.True(t, deleted)
}

func TestReceiptBudgetDrainsBeforeShutdownAndAcknowledges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var running, deleted, shutdown atomic.Bool
	client := receiptTestClient{receive: func(context.Context, *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		return receiptOutput(), nil
	}, delete: func(ctx context.Context, _ *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		require.NoError(t, ctx.Err())
		deleted.Store(true)
		return &sqs.DeleteMessageOutput{}, nil
	}}
	handler := receiptTestHandler{run: func(ctx context.Context, _ *Message) error {
		running.Store(true)
		close(started)
		<-release
		require.NoError(t, ctx.Err())
		running.Store(false)
		return nil
	}, shutdown: func() { require.False(t, running.Load()); require.True(t, deleted.Load()); shutdown.Store(true) }}
	c, err := NewConsumerWithSQSClient(client, receiptConfig(), handler)
	require.NoError(t, err)
	go func() { c.Consume(ctx); close(done) }()
	waitReceiptTest(t, started)
	cancel()
	close(release)
	waitReceiptTest(t, done)
	require.True(t, shutdown.Load())
}

func TestReceiptBudgetSkipsExpiredDispatchAndCapsCustomTimeout(t *testing.T) {
	timeout := time.Hour
	cfg := receiptConfig()
	cfg.HandlerTimeoutDuration = &timeout
	var calls int
	client := receiptTestClient{delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		return &sqs.DeleteMessageOutput{}, nil
	}}
	receiptDeadline := time.Now().Add(30 * time.Second)
	handler := receiptTestHandler{run: func(ctx context.Context, _ *Message) error {
		calls++
		deadline, _ := ctx.Deadline()
		require.Equal(t, receiptDeadline.Add(-6*time.Second), deadline)
		return nil
	}}
	c, err := NewConsumerWithSQSClient(client, cfg, handler)
	require.NoError(t, err)
	require.NoError(t, c.handleMsg(context.Background(), newMessage(&receiptOutput().Messages[0]), receiptDeadline))
	require.ErrorIs(t, c.handleMsg(context.Background(), newMessage(&receiptOutput().Messages[0]), time.Now().Add(-time.Second)), context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}

func TestReceiptBudgetReceiveDeadlineAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	client := receiptTestClient{receive: func(receiveCtx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		deadline, ok := receiveCtx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(30*time.Second), deadline, time.Second)
		cancel()
		<-receiveCtx.Done()
		return nil, receiveCtx.Err()
	}}
	c, err := NewConsumerWithSQSClient(client, receiptConfig(), nil)
	require.NoError(t, err)
	go func() { c.Consume(ctx); close(done) }()
	waitReceiptTest(t, done)
}

func TestReceiptBudgetFailureTelemetryOmitsReceiptHandle(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()
	apiErr := &smithy.GenericAPIError{Code: "InvalidParameterValue", Message: "receipt secret-receipt has expired"}
	client := receiptTestClient{delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) { return nil, apiErr }}
	handler := receiptTestHandler{run: func(context.Context, *Message) error { return nil }}
	c, err := NewConsumerWithSQSClient(client, receiptConfig(), handler)
	require.NoError(t, err)
	message := newMessage(&receiptOutput().Messages[0])
	require.Error(t, c.handleMsg(context.Background(), message, time.Now().Add(30*time.Second)))
	select {
	case handlerResult := <-message.err:
		require.NoError(t, handlerResult)
	case <-time.After(time.Second):
		t.Fatal("missing handler success response after acknowledgment failure")
	}
	require.Equal(t, 1, logs.Len())
	fields := logs.All()[0].ContextMap()
	require.Equal(t, "InvalidParameterValue", fields["error_code"])
	require.Equal(t, "DeleteMessage", fields["operation"])
	require.Equal(t, "test", fields["queue"])
	require.Contains(t, fields, "receipt_age_upper_bound")
	require.Contains(t, fields, "processing_duration")
	require.Contains(t, fields, "ack_duration")
	require.NotContains(t, logs.All()[0].Message, "secret-receipt")
	require.NotContains(t, fmt.Sprint(fields), "secret-receipt")
}

func TestReceiptBudgetHandlerFailureIsNotAcknowledged(t *testing.T) {
	var deleted bool
	client := receiptTestClient{delete: func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		deleted = true
		return nil, nil
	}}
	handlerErr := errors.New("test handler failure")
	handler := receiptTestHandler{run: func(context.Context, *Message) error { return handlerErr }}
	c, err := NewConsumerWithSQSClient(client, receiptConfig(), handler)
	require.NoError(t, err)
	require.ErrorIs(t, c.handleMsg(context.Background(), newMessage(&receiptOutput().Messages[0]), time.Now().Add(30*time.Second)), handlerErr)
	require.False(t, deleted)
}

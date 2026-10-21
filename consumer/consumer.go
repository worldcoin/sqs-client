package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

type Config struct {
	QueueURL                 string
	WorkersNum               int
	VisibilityTimeoutSeconds int32
	BatchSize                int32
	HandlerTimeoutDuration   *time.Duration
}

type SQSClient interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type Consumer struct {
	sqs     SQSClient
	handler Handler
	wg      *sync.WaitGroup
	cfg     Config
}

type receivedMessage struct {
	message  *Message
	deadline time.Time
}

func NewConsumer(awsCfg aws.Config, cfg Config, handler Handler) (*Consumer, error) {
	return NewConsumerWithSQSClient(sqs.NewFromConfig(awsCfg), cfg, handler)
}

func NewConsumerWithSQSClient(client SQSClient, cfg Config, handler Handler) (*Consumer, error) {
	if cfg.VisibilityTimeoutSeconds < 30 {
		return nil, errors.New("VisibilityTimeoutSeconds must be greater or equal to 30")
	}
	if cfg.WorkersNum < 1 {
		return nil, errors.New("WorkersNum must be positive")
	}
	if cfg.BatchSize < 1 || cfg.BatchSize > 10 {
		return nil, errors.New("BatchSize must be between 1 and 10")
	}
	if cfg.HandlerTimeoutDuration == nil {
		timeout := time.Duration(int32(float64(cfg.VisibilityTimeoutSeconds)*0.8)) * time.Second
		cfg.HandlerTimeoutDuration = &timeout
	}
	if *cfg.HandlerTimeoutDuration <= 0 {
		return nil, errors.New("HandlerTimeoutDuration must be positive")
	}
	return &Consumer{sqs: client, handler: handler, wg: &sync.WaitGroup{}, cfg: cfg}, nil
}

func (c *Consumer) Consume(ctx context.Context) {
	slots := make(chan struct{}, c.cfg.WorkersNum)
	for i := 0; i < c.cfg.WorkersNum; i++ {
		slots <- struct{}{}
	}
	jobs := make(chan receivedMessage, c.cfg.WorkersNum)
	for i := 0; i < c.cfg.WorkersNum; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for job := range jobs {
				_ = c.handleMsg(ctx, job.message, job.deadline) // handleMsg records failures with receipt timing.
				slots <- struct{}{}
			}
		}()
	}
	defer func() {
		close(jobs)
		c.wg.Wait()
		if c.handler != nil {
			c.handler.Shutdown()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-slots:
		}
		// Only intake removes slots; workers can only add capacity after this snapshot.
		reserved := min(int(c.cfg.BatchSize), 1+len(slots))
		for i := 1; i < reserved; i++ {
			<-slots
		}
		if ctx.Err() != nil {
			releaseSlots(slots, reserved)
			return
		}

		// Start before the remote call so network and dispatch time consume the budget.
		deadline := time.Now().Add(time.Duration(c.cfg.VisibilityTimeoutSeconds) * time.Second)
		receiveCtx, cancel := context.WithDeadline(ctx, deadline)
		output, err := c.sqs.ReceiveMessage(receiveCtx, &sqs.ReceiveMessageInput{
			QueueUrl: &c.cfg.QueueURL, MaxNumberOfMessages: int32(reserved), WaitTimeSeconds: 5,
			MessageAttributeNames: []string{"TraceID", "SpanID"}, VisibilityTimeout: c.cfg.VisibilityTimeoutSeconds,
		})
		cancel()
		if err != nil {
			releaseSlots(slots, reserved)
			if ctx.Err() != nil {
				return
			}
			logConsumerError("could not receive messages from SQS", err, zap.String("queue", c.cfg.QueueURL), zap.String("operation", "ReceiveMessage"))
			continue
		}
		if output == nil || len(output.Messages) > reserved {
			releaseSlots(slots, reserved)
			logConsumerError("invalid SQS receive response", errors.New("message count exceeds reserved capacity or response is nil"), zap.String("queue", c.cfg.QueueURL), zap.String("operation", "ReceiveMessage"))
			continue
		}
		releaseSlots(slots, reserved-len(output.Messages))
		// Reserved capacity guarantees the received batch fits; cancellation drains it.
		for _, msg := range output.Messages {
			localMsg := msg
			jobs <- receivedMessage{message: newMessage(&localMsg), deadline: deadline}
		}
	}
}

func releaseSlots(slots chan<- struct{}, n int) {
	for i := 0; i < n; i++ {
		slots <- struct{}{}
	}
}

func (c *Consumer) handleMsg(ctx context.Context, m *Message, deadline time.Time) (err error) {
	started := time.Now()
	operation := "handler"
	var processingDuration, ackDuration time.Duration
	defer func() {
		if err != nil {
			fields := []zap.Field{
				zap.String("queue", c.cfg.QueueURL), zap.String("operation", operation),
				zap.Duration("receipt_age_upper_bound", time.Since(deadline.Add(-time.Duration(c.cfg.VisibilityTimeoutSeconds)*time.Second))),
				zap.Duration("processing_duration", processingDuration), zap.Duration("ack_duration", ackDuration),
			}
			var apiErr smithy.APIError
			if operation == "handler" && !errors.As(err, &apiErr) {
				errorText := err.Error()
				if handle := aws.ToString(m.ReceiptHandle); handle != "" {
					errorText = strings.ReplaceAll(errorText, handle, "[REDACTED]")
				}
				fields = append(fields, zap.String("error", errorText))
			}
			logConsumerError("error processing SQS message", err, fields...)
		}
	}()
	// Intake cancellation stops new receives; accepted work drains within its receipt budget.
	receiptCtx, receiptCancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer receiptCancel()
	ackReserve := time.Duration(c.cfg.VisibilityTimeoutSeconds) * time.Second / 5
	handlerDeadline := deadline.Add(-ackReserve)
	if configured := time.Now().Add(*c.cfg.HandlerTimeoutDuration); configured.Before(handlerDeadline) {
		handlerDeadline = configured
	}
	handlerCtx, cancel := context.WithDeadline(receiptCtx, handlerDeadline)
	defer cancel()
	if err := receiptContextError(handlerCtx); err != nil {
		return m.ErrorResponse(err)
	}
	if c.handler != nil {
		if err := c.handler.Run(handlerCtx, m); err != nil {
			processingDuration = time.Since(started)
			return m.ErrorResponse(err)
		}
	}
	processingDuration = time.Since(started)
	// A handler that ignores cancellation must never acknowledge expired work.
	if err := receiptContextError(handlerCtx); err != nil {
		return m.ErrorResponse(err)
	}
	operation = "DeleteMessage"
	m.Success()
	ackStarted := time.Now()
	err = c.delete(receiptCtx, m)
	ackDuration = time.Since(ackStarted)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) delete(ctx context.Context, m *Message) error {
	if err := receiptContextError(ctx); err != nil {
		return err
	}
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.cfg.QueueURL, ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		return fmt.Errorf("unable to delete message from the queue: %w", err)
	}
	return nil
}

func receiptContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Context timer callbacks can lag behind wall-clock deadlines under CPU pressure.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func logConsumerError(message string, err error, fields ...zap.Field) {
	fields = append(fields, zap.String("error_type", fmt.Sprintf("%T", err)))
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		fields = append(fields, zap.String("error_code", apiErr.ErrorCode()))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		fields = append(fields, zap.String("cause", "deadline exceeded"))
	}
	if errors.Is(err, context.Canceled) {
		fields = append(fields, zap.String("cause", "canceled"))
	}
	// AWS error text can include receipt handles; only log the class and code.
	zap.L().Error(message, fields...)
}

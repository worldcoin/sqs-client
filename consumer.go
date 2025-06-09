package sqsclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/zap"
)

type Config struct {
	QueueURL                 string
	WorkersNum               int
	VisibilityTimeoutSeconds int32
	BatchSize                int32
	HandlerTimeoutSeconds    int32
	DeleteTimeoutSeconds     int32
}

type Consumer struct {
	sqs     *sqs.Client
	handler Handler
	wg      *sync.WaitGroup
	cfg     Config
}

func NewConsumer(awsCfg aws.Config, cfg Config, handler Handler) (*Consumer, error) {
	if cfg.VisibilityTimeoutSeconds < 30 {
		return nil, errors.New("VisibilityTimeoutSeconds must be greater or equal to 30")
	}

	// Set default handler timeout to 80% of visibility timeout if not specified
	if cfg.HandlerTimeoutSeconds == 0 {
		cfg.HandlerTimeoutSeconds = int32(float64(cfg.VisibilityTimeoutSeconds) * 0.8)
	}

	// Set default delete timeout to 15 seconds if not specified
	if cfg.DeleteTimeoutSeconds == 0 {
		cfg.DeleteTimeoutSeconds = 15
	}

	return &Consumer{
		sqs:     sqs.NewFromConfig(awsCfg),
		handler: handler,
		wg:      &sync.WaitGroup{},
		cfg:     cfg,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) {
	jobs := make(chan *Message)
	for w := 1; w <= c.cfg.WorkersNum; w++ {
		go c.worker(ctx, jobs)
		c.wg.Add(1)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			zap.S().Info("closing jobs channel")
			c.handler.Shutdown()
			close(jobs)
			break loop
		default:
			output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              &c.cfg.QueueURL,
				MaxNumberOfMessages:   c.cfg.BatchSize,
				WaitTimeSeconds:       int32(5),
				MessageAttributeNames: []string{"TraceID", "SpanID"},
				VisibilityTimeout:     c.cfg.VisibilityTimeoutSeconds,
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					// Suppress expected errors during shutdown
					zap.S().Warn("ReceiveMessage interrupted due to context cancellation")
					continue
				}
				zap.S().With(zap.Error(err)).Error("could not receive messages from SQS")
				continue
			}

			for _, msg := range output.Messages {
				localMsg := msg // Create a local copy to avoid reference issues
				jobs <- newMessage(&localMsg)
			}
		}
	}

	c.wg.Wait()
}

func (c *Consumer) worker(ctx context.Context, messages <-chan *Message) {
	for m := range messages {
		if err := c.handleMsg(ctx, m); err != nil {
			zap.S().With(zap.Error(err)).Error("error running handlers")
		}
	}
	zap.S().Info("worker exiting")
	c.wg.Done()
}

func (c *Consumer) handleMsg(ctx context.Context, m *Message) error {
	if c.handler != nil {
		// Create message-scoped context for handler execution
		handlerTimeout := time.Duration(c.cfg.HandlerTimeoutSeconds) * time.Second
		handlerCtx, cancel := context.WithTimeout(ctx, handlerTimeout)
		defer cancel()

		if err := c.handler.Run(handlerCtx, m); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				zap.S().Warn("handler execution timed out", zap.Duration("timeout", handlerTimeout))
			}
			return m.ErrorResponse(err)
		}
		m.Success()
	}

	// Use background context for cleanup to ensure it completes even during shutdown
	deleteTimeout := time.Duration(c.cfg.DeleteTimeoutSeconds) * time.Second
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), deleteTimeout)
	defer deleteCancel()
	return c.delete(deleteCtx, m) //MESSAGE CONSUMED
}

func (c *Consumer) delete(ctx context.Context, m *Message) error {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.cfg.QueueURL, ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		zap.S().With(zap.Error(err)).Error("error removing message")
		return fmt.Errorf("unable to delete message from the queue: %w", err)
	}
	zap.S().Debug("message deleted")
	return nil
}

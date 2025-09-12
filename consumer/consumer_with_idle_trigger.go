package consumer

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

type ConsumerWithIdleTrigger struct {
	sqs                       *sqs.Client
	handler                   sqsclient.HandlerWithIdleTrigger
	wg                        *sync.WaitGroup
	cfg                       Config
	idleDurationTimeout       time.Duration
	sqsReceiveWaitTimeSeconds int32
}

func NewConsumerWithIdleTrigger(awsCfg aws.Config, cfg Config, handler sqsclient.HandlerWithIdleTrigger, idleDurationTimeout time.Duration, sqsReceiveWaitTimeSeconds int32) (*ConsumerWithIdleTrigger, error) {
	if cfg.VisibilityTimeoutSeconds < 30 {
		return nil, errors.New("VisibilityTimeoutSeconds must be greater or equal to 30")
	}
	return &ConsumerWithIdleTrigger{
		sqs:                       sqs.NewFromConfig(awsCfg),
		handler:                   handler,
		wg:                        &sync.WaitGroup{},
		cfg:                       cfg,
		idleDurationTimeout:       idleDurationTimeout,
		sqsReceiveWaitTimeSeconds: sqsReceiveWaitTimeSeconds,
	}, nil
}

func (c *ConsumerWithIdleTrigger) Consume(ctx context.Context) {
	jobs := make(chan *Message)
	for w := 1; w <= c.cfg.WorkersNum; w++ {
		go c.worker(ctx, jobs)
		c.wg.Add(1)
	}
	timeout := time.NewTimer(c.idleDurationTimeout)
	defer timeout.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			zap.S().Info("closing jobs channel")
			c.handler.Shutdown()
			close(jobs)
			break loop
		case <-timeout.C:
			zap.S().Info("No new messages for timeout duration, triggering timeout logic")
			// a nil message should trigger a timeout
			jobs <- sqsclient.newMessage(nil)
			// Reset the timeout
			timeout.Reset(c.idleDurationTimeout)
		default:
			output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              &c.cfg.QueueURL,
				MaxNumberOfMessages:   c.cfg.BatchSize,
				WaitTimeSeconds:       c.sqsReceiveWaitTimeSeconds,
				MessageAttributeNames: []string{"TraceID", "SpanID"},
				VisibilityTimeout:     c.cfg.VisibilityTimeoutSeconds,
			})
			if err != nil {
				zap.S().With(zap.Error(err)).Error("could not receive messages from SQS")
				continue
			}
			if len(output.Messages) > 0 {
				// Reset the timeout since we received messages
				if !timeout.Stop() {
					select {
					case <-timeout.C:
					default:
					}
				}
				timeout.Reset(c.idleDurationTimeout)
			}

			for _, m := range output.Messages {
				jobs <- sqsclient.newMessage(&m)
			}
		}
	}

	c.wg.Wait()
}

func (c *ConsumerWithIdleTrigger) worker(ctx context.Context, messages <-chan *Message) {
	for m := range messages {
		if err := c.handleMsg(ctx, m); err != nil {
			zap.S().With(zap.Error(err)).Error("error running handlers")
		}
	}
	zap.S().Info("worker exiting")
	c.wg.Done()
}

func (c *ConsumerWithIdleTrigger) handleMsg(ctx context.Context, m *Message) error {
	if c.handler != nil {
		if m.Message == nil {
			if err := c.handler.IdleTimeout(ctx); err != nil {
				return m.ErrorResponse(err)
			}
		} else {
			if err := c.handler.Run(ctx, m); err != nil {
				return m.ErrorResponse(err)
			}
			return c.delete(ctx, m) // Message consumed
		}
	}
	m.Success()
	return nil
}

func (c *ConsumerWithIdleTrigger) delete(ctx context.Context, m *Message) error {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.cfg.QueueURL, ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		zap.S().With(zap.Error(err)).Error("error removing message")
		return fmt.Errorf("unable to delete message from the queue: %w", err)
	}
	zap.S().Debug("message deleted")
	return nil
}

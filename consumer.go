package sqsclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"go.uber.org/zap"
)

type Config struct {
	QueueURL          string
	WorkersNum        int
	VisibilityTimeout int32
	WaitTimeSecs      int32
	BatchSize         int32
	ExtendEnabled     bool
}

type Consumer struct {
	sqs        *sqs.Client
	handlers   []Handler
	handlersWg *sync.WaitGroup
	cfg        Config
	terminated bool
}

func NewConsumer(awsCfg aws.Config, cfg Config, handlerFactory HandlerFactory) *Consumer {
	handlers := make([]Handler, cfg.WorkersNum)
	for i := 0; i < cfg.WorkersNum; i++ {
		handlers[i] = newGracefulShutdownDecorator(handlerFactory.NewHandler())
	}
	return &Consumer{
		sqs:        sqs.NewFromConfig(awsCfg),
		handlers:   handlers,
		handlersWg: &sync.WaitGroup{},
		cfg:        cfg,
	}
}

func (c *Consumer) Consume(ctx context.Context) {
	messages := make(chan *Message)
	for w := 0; w < c.cfg.WorkersNum; w++ {
		// Assign handler to messages channel
		handler := c.handlers[w]
		go c.worker(ctx, messages, handler)
		c.handlersWg.Add(1)
	}

	go c.triggerShutdownWhenCtxCancelled(ctx)

loop:
	for {
		select {
		case <-ctx.Done():
			// Closing messages channel to signal workers to stop. We need to close the channel in the same goroutine where the sending is done, to avoid panic caused by sending on a closed channel.
			close(messages)
			break loop
		default:
			output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              &c.cfg.QueueURL,
				MaxNumberOfMessages:   c.cfg.BatchSize,
				WaitTimeSeconds:       c.cfg.WaitTimeSecs,
				MessageAttributeNames: []string{"TraceID", "SpanID"},
			})
			if err != nil {
				zap.S().With(zap.Error(err)).Error("could not receive messages from SQS")
				continue
			}

			for _, m := range output.Messages {
				messages <- newMessage(&m)
			}
		}
	}

	c.handlersWg.Wait()
}

func (c *Consumer) worker(ctx context.Context, messages <-chan *Message, handler Handler) {
	for {
		select {
		case m := <-messages:
			if c.terminated {
				// We need to use this attribute to avoid processing messages after the consumer has been terminated.
				// If instead of this, we checked if the channel was closed, we'd be processing one additional message when shutting down because the messages channel is unbuffered.
				// That means that the loop in the Consume function would be blocked on the send of the message until one worker picks it up, then, if ctx is done, it would close the messages
				// channel. That means that, the worker would process one last message even if it arrived after the ctx was cancelled. The loop would break after that last message is sent to the channell,
				// but the worker would still process that message.
				zap.S().Info("consumer terminated, worker exiting")
				return
			}
			ctxHandler, cancel := context.WithTimeout(context.Background(), time.Duration(c.cfg.VisibilityTimeout)*time.Second)
			if err := c.handleMsg(ctxHandler, m, handler); err != nil {
				zap.S().Error("error running handlers", err)
			}
			cancel()
		case <-ctx.Done():
			zap.S().Info("context cancelled, worker exiting")
			return
		}
	}
}

func (c *Consumer) handleMsg(ctx context.Context, m *Message, handler Handler) error {
	if c.cfg.ExtendEnabled {
		c.extend(ctx, m)
	}
	if err := handler.Run(ctx, m); err != nil {
		return m.ErrorResponse(err)
	}
	m.Success()

	return c.delete(ctx, m) //MESSAGE CONSUMED
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

func (c *Consumer) extend(ctx context.Context, m *Message) {
	_, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &c.cfg.QueueURL,
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: c.cfg.VisibilityTimeout,
	})
	if err != nil {
		zap.S().With(zap.Error(err)).Error("unable to extend message")
		return
	}
}

// shutdownAllHandlers calls Handler.Shutdown on all handlers concurrently and waits for them to finish. This is a blocking call.
// Here we rely on the handlers to use a context with timeout to avoid blocking indefinitely.
// This assumption is met because the worker function is creating a context with a timeout for each message handled.
func (c *Consumer) shutdownAllHandlers() {
	wg := sync.WaitGroup{}
	wg.Add(len(c.handlers))
	for _, handler := range c.handlers {
		go func(handler Handler) {
			handler.Shutdown()
			wg.Done()
		}(handler)
	}
	wg.Wait()
}

func (c *Consumer) triggerShutdownWhenCtxCancelled(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			zap.S().Info("shutting down sqs-client...")
			c.terminated = true
			c.shutdownAllHandlers()
			c.handlersWg.Done()
			return
		}
	}
}

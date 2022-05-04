package sqsclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	sqs               *sqs.Client
	handler           Handler
	queueURL          string
	visibilityTimeout int32
	batchSize         int32
	workersNum        int
	handlerTimeout    int
	wg                *sync.WaitGroup
}

func NewConsumer(cfg aws.Config, queueURL string, visibilityTimeout, batchSize, workersNum, handlerTimeout int, handler Handler) *Consumer {
	consumer := &Consumer{
		sqs:               sqs.NewFromConfig(cfg),
		handler:           handler,
		queueURL:          queueURL,
		visibilityTimeout: int32(visibilityTimeout),
		batchSize:         int32(batchSize),
		workersNum:        workersNum,
		handlerTimeout:    handlerTimeout,
		wg:                &sync.WaitGroup{},
	}

	return consumer
}

func (c *Consumer) Consume(ctx context.Context) {
	jobs := make(chan *Message, c.workersNum)
	for w := 1; w <= c.workersNum; w++ {
		go c.worker(ctx, jobs, w)
		c.wg.Add(1)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("closing jobs channel")
			close(jobs)
			break loop
		default:
			output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            &c.queueURL,
				MaxNumberOfMessages: c.batchSize,
				WaitTimeSeconds:     int32(5),
			})

			if output != nil {
				log.Infof("SQS Client received %d messages", len(output.Messages))
			} else {
				log.Info("No messages received")
			}
			if err != nil {
				log.WithError(err).Error("could not receive messages from SQS")
				continue
			}

			for _, m := range output.Messages {
				jobs <- newMessage(&m)
			}
		}
	}

	c.wg.Wait()
}

func (c *Consumer) worker(ctx context.Context, messages <-chan *Message, workerId int) {
	for m := range messages {
		if err := c.handleMsg(ctx, m, workerId); err != nil {
			log.WithError(err).Error("error running handlers")
		}
	}
	log.Info("worker exiting")
	c.wg.Done()
}

func (c *Consumer) handleMsg(ctx context.Context, m *Message, workerId int) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "sqs_client.handle_msg")
	span.SetTag("worker_id", workerId)
	defer span.Finish()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(c.handlerTimeout)*time.Second)
	defer cancel()

	go func() {
		select {
		case <-ctxWithTimeout.Done():
			if ctxWithTimeout.Err() != nil && ctxWithTimeout.Err() == context.DeadlineExceeded {
				log.WithError(ctxWithTimeout.Err()).Error("SQS message handler timed out")
			} else {
				log.Info("SQS message handler completed")
			}
			break
		}
	}()

	c.extend(ctxWithTimeout, m)
	if err := c.handler.Run(ctxWithTimeout, m); err != nil {
		span.SetTag("success", false)
		span.SetTag("error", err)
		return m.ErrorResponse(err)
	}
	m.Success()

	err := c.delete(ctxWithTimeout, m)
	if err != nil {
		span.SetTag("success", false)
		span.SetTag("error", err)
		return err
	}

	span.SetTag("success", true)
	return nil
}

func (c *Consumer) delete(ctx context.Context, m *Message) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "sqs_client.delete_msg")
	defer span.Finish()
	log.WithFields(TraceFields(ctx)).Info("deleting message from SQS")
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.queueURL, ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		log.WithFields(TraceFields(ctx)).WithError(err).Error("error removing message")
		span.SetTag("deleted", false)
		span.SetTag("error", err)
		return fmt.Errorf("unable to delete message from the queue: %w", err)
	}
	span.SetTag("deleted", true)
	log.WithFields(TraceFields(ctx)).Info("message deleted")
	return nil
}

func (c *Consumer) extend(ctx context.Context, m *Message) {
	_, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &c.queueURL,
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: c.visibilityTimeout,
	})
	if err != nil {
		log.WithFields(TraceFields(ctx)).WithError(err).Error("unable to extend message")
		return
	}
}

func TraceFields(ctx context.Context) log.Fields {
	if span, ok := tracer.SpanFromContext(ctx); ok {
		return log.Fields{"dd.trace_id": span.Context().TraceID(), "dd.span_id": span.Context().SpanID()}
	}
	return log.Fields{}
}

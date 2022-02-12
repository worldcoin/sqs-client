package sqsclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	sqs               *sqs.Client
	handler           Handler
	queueURL          string
	workersNum        int
	visibilityTimeout int32
	batchSize         int32
	wg                *sync.WaitGroup
}

func NewConsumer(ctx context.Context, cfg aws.Config, queueName string, visibilityTimeout, batchSize, workersNum int, handler Handler) (*Consumer, error) {
	sqsSvc := sqs.NewFromConfig(cfg)

	queueUrlOut, err := sqsSvc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return nil, fmt.Errorf("error getting queueUrl")
	}

	consumer := &Consumer{
		sqs:               sqs.NewFromConfig(cfg),
		handler:           handler,
		queueURL:          *queueUrlOut.QueueUrl,
		workersNum:        workersNum,
		visibilityTimeout: int32(visibilityTimeout),
		batchSize:         int32(batchSize),
		wg:                &sync.WaitGroup{},
	}

	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context) {
	jobs := make(chan *Message)
	for w := 1; w <= c.workersNum; w++ {
		go c.worker(ctx, jobs)
		c.wg.Add(1)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			break loop
		default:
			output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            &c.queueURL,
				MaxNumberOfMessages: c.batchSize,
				WaitTimeSeconds:     int32(5),
			})
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

func (c *Consumer) worker(ctx context.Context, messages <-chan *Message) {
	for m := range messages {
		if err := c.handleMsg(ctx, m); err != nil {
			log.WithError(err).Error("error running handlers")
		}
	}
	c.wg.Done()
}

func (c *Consumer) handleMsg(ctx context.Context, m *Message) error {
	if c.handler != nil {
		c.extend(ctx, m)
		if err := c.handler.Run(ctx, m); err != nil {
			return m.ErrorResponse(err)
		}
		m.Success()
	}

	return c.delete(ctx, m) //MESSAGE CONSUMED
}

func (c *Consumer) delete(ctx context.Context, m *Message) error {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.queueURL, ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		log.WithError(err).Error("error removing message")
		return fmt.Errorf("unable to delete message from the queue: %w", err)
	}
	return nil
}

func (c *Consumer) extend(ctx context.Context, m *Message) {
	_, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &c.queueURL,
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: c.visibilityTimeout,
	})
	if err != nil {
		log.WithError(err).Error("unable to extend message")
		return
	}
}

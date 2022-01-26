package sqs

import (
    "context"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"
    log "github.com/sirupsen/logrus"
)

type Config struct {
    AwsAccessKey      string
    AwsSecretKey      string
    AwsRegion         string
    QueueHost         string
    WorkerPool        int
    VisibilityTimeout int
    DisableSSL        bool
}

type Consumer struct {
    SQS               *sqs.SQS
    handler           Handler
    QueueURL          string
    VisibilityTimeout int
    extensionLimit    int
    workerPool        int
    receiveMessages   int64
    done              bool
}

func NewConsumer(c Config, queueName string) (*Consumer, error) {
    sess, err := newSession(c)
    if err != nil {
        return nil, err
    }
    cfgs := sqsEndpointConfig(c)

    cons := &Consumer{
        SQS:               sqs.New(sess, cfgs),
        VisibilityTimeout: c.VisibilityTimeout,
        extensionLimit:    2,
        workerPool:        c.WorkerPool,
        receiveMessages:   10,
        done:              false,
    }

    o, err := cons.SQS.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &queueName})
    if err != nil {
        return nil, err
    }
    cons.QueueURL = *o.QueueUrl

    return cons, nil
}

func CreateQueue(c Config, queueName string) (*sqs.CreateQueueOutput, error) {
    sess, err := newSession(c)
    if err != nil {
        return nil, err
    }
    cfgs := sqsEndpointConfig(c)

    SQS := sqs.New(sess, cfgs)

    queue, err := SQS.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String(queueName)})
    if err != nil {
        return nil, err
    }

    return queue, nil
}

func (c *Consumer) RegisterHandler(h Handler) {
    c.handler = h
}

func (c *Consumer) Consume() {
    jobs := make(chan *Message)
    for w := 1; w <= c.workerPool; w++ {
        go c.worker(jobs)
    }

    for {
        if c.done {
            return
        }
        output, err := c.SQS.ReceiveMessage(&sqs.ReceiveMessageInput{
            QueueUrl:            &c.QueueURL,
            MaxNumberOfMessages: &c.receiveMessages})
        if err != nil {
            log.WithError(err).Error("could not receive messages from SQS")
            time.Sleep(10 * time.Second)
            continue
        }

        for _, m := range output.Messages {
            jobs <- newMessage(m)
        }
    }
}

func (c *Consumer) Stop() {
    c.done = true
}

func (c *Consumer) worker(messages <-chan *Message) {
    for m := range messages {
        if err := c.run(m); err != nil {
            log.WithError(err).Error("error running handlers")
        }
    }
}

func (c *Consumer) run(m *Message) error {
    if c.handler != nil {
        ctx := context.Background()

        go c.extend(ctx, m)
        if err := c.handler(ctx, m); err != nil {
            return m.ErrorResponse(err)
        }
        m.Success()
    }

    return c.delete(m) //MESSAGE CONSUMED
}

func (c *Consumer) delete(m *Message) error {
    _, err := c.SQS.DeleteMessage(&sqs.DeleteMessageInput{QueueUrl: &c.QueueURL, ReceiptHandle: m.ReceiptHandle})
    if err != nil {
        log.WithError(err).Error("error removing message")
        return fmt.Errorf("unable to delete message from the queue: %w", err)
    }
    return nil
}

func (c *Consumer) extend(ctx context.Context, m *Message) {
    var count int
    extension := int64(c.VisibilityTimeout)
    for {
        if count >= c.extensionLimit {
            log.WithField("extensionLimit", c.extensionLimit).Error("extensions limit is exceeded")
            return
        }

        count++
        time.Sleep(time.Duration(c.VisibilityTimeout-10) * time.Second)
        select {
        case <-ctx.Done():
            return
        case <-m.err:
            // goroutine finished
            return
        default:
            extension = extension + int64(c.VisibilityTimeout)
            _, err := c.SQS.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{QueueUrl: &c.QueueURL, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: &extension})
            if err != nil {
                log.WithError(err).Error("unable to extend")
                return
            }
        }
    }
}

func sqsEndpointConfig(conf Config) *aws.Config {
    awsConfig := &aws.Config{}
    if conf.QueueHost != "" {
        awsConfig.Endpoint = aws.String(conf.QueueHost)
    }
    awsConfig.DisableSSL = aws.Bool(conf.DisableSSL)
    return awsConfig
}

package sqs

import (
    "context"
    "encoding/json"
    "errors"
    "os"
    "strconv"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"
    log "github.com/sirupsen/logrus"
    "github.com/stretchr/testify/assert"
)

var conf = Config{
    AwsAccessKey:      "sqs",
    AwsSecretKey:      "sqs",
    AwsRegion:         "us-east-1",
    QueueHost:         "http://localhost:9324",
    WorkerPool:        1,
    VisibilityTimeout: 15,
    DisableSSL:        true,
}

type TestMsg struct {
    Name string `json:"name"`
}

func TestMain(m *testing.M) {
    queueHost, found := os.LookupEnv("QUEUE_HOST")
    if found {
        conf.QueueHost = queueHost
    }
    code := m.Run()
    os.Exit(code)
}

func TestConsume(t *testing.T) {
    var receivedMsg TestMsg
    msgsReceivedCount := 0
    handler := func(ctx context.Context, m *Message) error {
        msgsReceivedCount += 1
        err := json.Unmarshal(m.body(), &receivedMsg)
        if err != nil {
            log.Error("error unmarshalling message")
            t.FailNow()
        }
        return err
    }

    queue, consumer := createQueueAndConsumer(t, handler)

    t.Cleanup(func() {
        consumer.Stop()
        _, err := consumer.SQS.PurgeQueue(&sqs.PurgeQueueInput{QueueUrl: queue.QueueUrl})
        if err != nil {
            log.Error("failed to purge queue")
            t.FailNow()
        }
    })

    // Send message to the queue
    expectedMsg := sendTestMsg(t, consumer, queue)

    // Wait for the message to arrive
    time.Sleep(time.Second * 1)

    // Check that the message arrived
    assert.Equal(t, 1, msgsReceivedCount)
    assert.Equal(t, expectedMsg, receivedMsg)

    // Check that received message was deleted from the queue
    messageCount := getNumOfVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 0, messageCount)
    messageCount = getNumOfNotVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 0, messageCount)
}

func TestConsume_WhenHandlerIsDelayed_MessageIsExtended(t *testing.T) {
    msgsReceivedCount := 0
    handler := func(ctx context.Context, m *Message) error {
        msgsReceivedCount += 1
        time.Sleep(time.Second * 20)
        return nil
    }

    queue, consumer := createQueueAndConsumer(t, handler)

    t.Cleanup(func() {
        consumer.Stop()
        _, err := consumer.SQS.PurgeQueue(&sqs.PurgeQueueInput{QueueUrl: queue.QueueUrl})
        if err != nil {
            log.Error("failed to purge queue")
            t.FailNow()
        }
    })

    // Send message to the queue
    sendTestMsg(t, consumer, queue)

    // Wait for the message to be extended at least once
    time.Sleep(time.Second * 12)

    // Check that the message arrived
    assert.True(t, msgsReceivedCount == 1)

    // Check that received message was not deleted from the queue because handler hasn't completed yet
    messageCount := getNumOfVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 0, messageCount)
    messageCount = getNumOfNotVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 1, messageCount)
}

func TestConsume_WhenHandlerFails_HandlerIsRetried(t *testing.T) {
    msgsReceivedCount := 0
    handler := func(ctx context.Context, m *Message) error {
        msgsReceivedCount += 1
        return errors.New("fake error")
    }

    queue, consumer := createQueueAndConsumer(t, handler)

    t.Cleanup(func() {
        consumer.Stop()
        _, err := consumer.SQS.PurgeQueue(&sqs.PurgeQueueInput{QueueUrl: queue.QueueUrl})
        if err != nil {
            log.Error("failed to purge queue")
            t.FailNow()
        }
    })

    // Send message to the queue
    sendTestMsg(t, consumer, queue)

    // Wait for the message to arrive
    time.Sleep(time.Second * 40)

    // Check that the handler was called twice (because when it fails it's retried every 30s)
    assert.True(t, msgsReceivedCount == 2)

    // Check that received message was not deleted because it's still being retried
    messageCount := getNumOfVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 0, messageCount)
    messageCount = getNumOfNotVisibleMessagesInQueue(t, consumer, queue)
    assert.Equal(t, 1, messageCount)
}

func createQueueAndConsumer(t *testing.T, handler func(ctx context.Context, m *Message) error) (*sqs.CreateQueueOutput, *Consumer) {
    queueName := "test_queue"

    queue, err := CreateQueue(conf, queueName)
    if err != nil {
        log.Error("error while creating queue")
        t.FailNow()
    }

    consumer, err := NewConsumer(conf, queueName)
    if err != nil {
        log.Error("error while creating consumer")
        t.FailNow()
    }
    consumer.RegisterHandler(handler)
    go consumer.Consume()
    return queue, consumer
}

func sendTestMsg(t *testing.T, consumer *Consumer, queue *sqs.CreateQueueOutput) TestMsg {
    expectedMsg := TestMsg{Name: "TestName"}
    messageBodyBytes, err := json.Marshal(expectedMsg)
    _, err = consumer.SQS.SendMessage(&sqs.SendMessageInput{
        MessageBody: aws.String(string(messageBodyBytes)),
        QueueUrl:    queue.QueueUrl,
    })
    if err != nil {
        log.Error("error sending message")
        t.FailNow()
    }
    return expectedMsg
}

func getNumOfVisibleMessagesInQueue(t *testing.T, consumer *Consumer, queue *sqs.CreateQueueOutput) int {
    return getQueueAttribute(t, consumer, queue, "ApproximateNumberOfMessages")
}

func getNumOfNotVisibleMessagesInQueue(t *testing.T, consumer *Consumer, queue *sqs.CreateQueueOutput) int {
    return getQueueAttribute(t, consumer, queue, "ApproximateNumberOfMessagesNotVisible")
}

func getQueueAttribute(t *testing.T, consumer *Consumer, queue *sqs.CreateQueueOutput, attributeName string) int {
    attributes, err := consumer.SQS.GetQueueAttributes(&sqs.GetQueueAttributesInput{
        QueueUrl:       queue.QueueUrl,
        AttributeNames: []*string{aws.String(attributeName)},
    })
    if err != nil {
        log.Error("error retrieving queue attributes")
        t.FailNow()
    }
    messageCount, err := strconv.Atoi(*attributes.Attributes[attributeName])
    if err != nil {
        log.Error("error converting string to int")
    }
    return messageCount
}

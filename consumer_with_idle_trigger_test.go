package sqsclient

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stretchr/testify/assert"
)

type MsgHandlerWithIdleTrigger struct {
	t                     *testing.T
	expectedMsg           TestMsg
	expectedMsgAttributes interface{}
	shutdownReceived      bool

	msgsReceivedCount         int
	idleTimeoutTriggeredCount int
}

const (
	IdleTimeout               = 500 * time.Millisecond
	SqsReceiveWaitTimeSeconds = int32(1)
)

func TestConsumeWithIdleTrigger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	awsCfg := loadAWSDefaultConfig(ctx)

	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	expectedMsg := TestMsg{Name: "TestName"}
	expectedMsgAttributes := map[string]types.MessageAttributeValue{
		"TraceID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(traceId),
		},
		"SpanID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(spanId),
		},
	}

	msgHandler := handlerWithIdleTrigger(t, expectedMsg, expectedMsgAttributes)
	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               workersNum,
		VisibilityTimeoutSeconds: visibilityTimeout,
		BatchSize:                batchSize,
	}
	consumer, err := NewConsumerWithIdleTrigger(awsCfg, config, msgHandler, IdleTimeout, SqsReceiveWaitTimeSeconds)
	assert.NoError(t, err)
	go consumer.Consume(ctx)

	t.Cleanup(func() {
		_, err := consumer.sqs.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queueUrl})
		if err != nil {
			zap.S().Error("failed to purge queue")
			t.FailNow()
		}
		cancel()
	})

	// Send message to the queue
	sendTestMsg(t, ctx, consumer.sqs, queueUrl, expectedMsg)

	// Wait for the message to arrive
	time.Sleep(time.Second * 1)

	// Check that the message arrived
	assert.Equal(t, 1, msgHandler.msgsReceivedCount)

	// Check that received message was deleted from the queue
	messageCount := getNumOfVisibleMessagesInQueue(t, ctx, consumer.sqs, queueUrl)
	assert.Equal(t, 0, messageCount)
	messageCount = getNumOfNotVisibleMessagesInQueue(t, ctx, consumer.sqs, queueUrl)
	assert.Equal(t, 0, messageCount)
}

func TestConsumeWithIdleTimeout_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	awsCfg := loadAWSDefaultConfig(ctx)

	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               workersNum,
		VisibilityTimeoutSeconds: visibilityTimeout,
		BatchSize:                batchSize,
	}
	msgHandler := MsgHandlerWithIdleTrigger{
		t:                 t,
		msgsReceivedCount: 0,
	}
	consumer, err := NewConsumerWithIdleTrigger(awsCfg, config, &msgHandler, IdleTimeout, SqsReceiveWaitTimeSeconds)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 1)
		// Cancel context to trigger graceful shutdown
		cancel()
	}()
	go func() {
		defer wg.Done()
		select {
		case <-time.After(time.Second * 5):
			zap.S().Error("consumer didn't shut down")
			t.Fatal("consumer failed to shut down gracefully within the expected time")
		case <-ctx.Done():
			zap.S().Info("test context done")
		}
	}()
	// Start consuming messages in a separate goroutine to prevent blocking
	go func() {
		consumer.Consume(ctx)
	}()

	wg.Wait()

	assert.Eventually(t, func() bool {
		// Check that shutdown was called
		return msgHandler.shutdownReceived
	}, time.Second*2, time.Millisecond*100)
}

func TestConsumeWithIdleTimeout_TimesOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	awsCfg := loadAWSDefaultConfig(ctx)

	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               workersNum,
		VisibilityTimeoutSeconds: visibilityTimeout,
		BatchSize:                batchSize,
	}
	msgHandler := MsgHandlerWithIdleTrigger{
		t:                 t,
		msgsReceivedCount: 0,
	}
	consumer, err := NewConsumerWithIdleTrigger(awsCfg, config, &msgHandler, IdleTimeout, SqsReceiveWaitTimeSeconds)
	assert.NoError(t, err)
	go consumer.Consume(ctx)

	t.Cleanup(func() {
		cancel()
	})

	// Wait for the timeout
	time.Sleep(time.Second * 3)

	// ensure that it gets called multiple times
	assert.GreaterOrEqual(t, msgHandler.idleTimeoutTriggeredCount, 2)
}

func TestConsumeWithIdleTimeout_ErrorsIfConfigIssues(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	awsCfg := loadAWSDefaultConfig(ctx)

	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	msgHandler := MsgHandlerWithIdleTrigger{
		t:                 t,
		msgsReceivedCount: 0,
	}
	tests := []struct {
		name                     string
		visibilityTimeoutSeconds int32
	}{
		{
			name:                     "VisibilityTimeoutSeconds is less than 30",
			visibilityTimeoutSeconds: int32(29),
		},
		{
			name:                     "VisibilityTimeoutSeconds is less than 0",
			visibilityTimeoutSeconds: int32(-1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				QueueURL:                 *queueUrl,
				WorkersNum:               workersNum,
				VisibilityTimeoutSeconds: tt.visibilityTimeoutSeconds,
				BatchSize:                batchSize,
			}
			consumer, err := NewConsumerWithIdleTrigger(awsCfg, config, &msgHandler, IdleTimeout, SqsReceiveWaitTimeSeconds)
			t.Logf("error: %v", err)
			assert.Error(t, err)
			assert.Nil(t, consumer)
		})
	}
}
func TestConsumeWithIdleTimeout_TimesOutAndConsumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	awsCfg := loadAWSDefaultConfig(ctx)

	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	expectedMsg := TestMsg{Name: "TestName"}
	expectedMsgAttributes := map[string]types.MessageAttributeValue{
		"TraceID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(traceId),
		},
		"SpanID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(spanId),
		},
	}

	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               workersNum,
		VisibilityTimeoutSeconds: visibilityTimeout,
		BatchSize:                batchSize,
	}
	msgHandler := handlerWithIdleTrigger(t, expectedMsg, expectedMsgAttributes)
	consumer, err := NewConsumerWithIdleTrigger(awsCfg, config, msgHandler, IdleTimeout, SqsReceiveWaitTimeSeconds)
	assert.NoError(t, err)
	go consumer.Consume(ctx)

	t.Cleanup(func() {
		_, err := consumer.sqs.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queueUrl})
		if err != nil {
			zap.S().Error("failed to purge queue")
			t.FailNow()
		}
		cancel()
	})
	time.Sleep(time.Second * 2)

	// ensure that it gets called first before receiving a message
	assert.GreaterOrEqual(t, msgHandler.idleTimeoutTriggeredCount, 1)
	assert.Equal(t, 0, msgHandler.msgsReceivedCount)

	// Send message to the queue
	sendTestMsg(t, ctx, consumer.sqs, queueUrl, expectedMsg)

	time.Sleep(time.Second * 2)
	// Check that the message arrived
	assert.Equal(t, 1, msgHandler.msgsReceivedCount)
	assert.GreaterOrEqual(t, msgHandler.idleTimeoutTriggeredCount, 2)

}

func handlerWithIdleTrigger(t *testing.T, expectedMsg TestMsg, expectedMsgAttributes map[string]types.MessageAttributeValue) *MsgHandlerWithIdleTrigger {
	return &MsgHandlerWithIdleTrigger{
		t:                         t,
		msgsReceivedCount:         0,
		expectedMsg:               expectedMsg,
		expectedMsgAttributes:     expectedMsgAttributes,
		idleTimeoutTriggeredCount: 0,
	}
}

func (m *MsgHandlerWithIdleTrigger) Run(ctx context.Context, msg *Message) error {
	zap.S().Info("running message")
	m.msgsReceivedCount += 1
	var actualMsg TestMsg
	err := json.Unmarshal(msg.body(), &actualMsg)
	if err != nil {
		zap.S().Error("error unmarshalling message")
		m.t.FailNow()
	}

	assert.EqualValues(m.t, m.expectedMsgAttributes, msg.MessageAttributes)

	// Check that the message received is the expected one
	assert.Equal(m.t, m.expectedMsg, actualMsg)

	return err
}

func (m *MsgHandlerWithIdleTrigger) Shutdown() {
	zap.S().Info("Shutting down")
	m.shutdownReceived = true
	// Do nothing
}

func (m *MsgHandlerWithIdleTrigger) IdleTimeout(ctx context.Context) error {
	zap.S().Info("Idle timeout triggered: ", m.idleTimeoutTriggeredCount)
	m.idleTimeoutTriggeredCount++
	return nil
}

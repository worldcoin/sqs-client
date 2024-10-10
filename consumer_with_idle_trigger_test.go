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
	msgsReceivedCount     int
	expectedMsg           TestMsg
	expectedMsgAttributes interface{}
	shutdownReceived      bool
	idleTimeoutTriggered  bool
}

const (
	IdleTimeout = 1 * time.Second
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
		QueueURL:          *queueUrl,
		WorkersNum:        workersNum,
		VisibilityTimeout: visibilityTimeout,
		BatchSize:         batchSize,
		ExtendEnabled:     true,
	}
	consumer := NewConsumerWithIdleTrigger(awsCfg, config, msgHandler, IdleTimeout)
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
		QueueURL:          *queueUrl,
		WorkersNum:        workersNum,
		VisibilityTimeout: visibilityTimeout,
		BatchSize:         batchSize,
		ExtendEnabled:     true,
	}
	msgHandler := MsgHandlerWithIdleTrigger{
		t:                 t,
		msgsReceivedCount: 0,
	}
	consumer := NewConsumerWithIdleTrigger(awsCfg, config, &msgHandler, IdleTimeout)
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
		QueueURL:          *queueUrl,
		WorkersNum:        workersNum,
		VisibilityTimeout: visibilityTimeout,
		BatchSize:         batchSize,
		ExtendEnabled:     true,
	}
	msgHandler := MsgHandlerWithIdleTrigger{
		t:                 t,
		msgsReceivedCount: 0,
	}
	consumer := NewConsumerWithIdleTrigger(awsCfg, config, &msgHandler, IdleTimeout)
	go consumer.Consume(ctx)

	t.Cleanup(func() {
		cancel()
	})

	// Wait for the timeout
	time.Sleep(time.Second * 2)

	assert.True(t, msgHandler.idleTimeoutTriggered)
}

func handlerWithIdleTrigger(t *testing.T, expectedMsg TestMsg, expectedMsgAttributes map[string]types.MessageAttributeValue) *MsgHandlerWithIdleTrigger {
	return &MsgHandlerWithIdleTrigger{
		t:                     t,
		msgsReceivedCount:     0,
		expectedMsg:           expectedMsg,
		expectedMsgAttributes: expectedMsgAttributes,
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
	zap.S().Info("Idle timeout triggered")
	m.idleTimeoutTriggered = true
	return nil
}

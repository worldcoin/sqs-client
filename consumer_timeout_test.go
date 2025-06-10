package sqsclient

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

type TimeoutTestHandler struct {
	t                 *testing.T
	msgsReceivedCount int
	shutdownReceived  bool
	processingDelay   time.Duration
	timeoutOccurred   bool
}

func (m *TimeoutTestHandler) Run(ctx context.Context, msg *Message) error {
	m.msgsReceivedCount++

	if m.processingDelay > 0 {
		select {
		case <-time.After(m.processingDelay):
			return nil
		case <-ctx.Done():
			m.timeoutOccurred = true
			return ctx.Err()
		}
	}
	return nil
}

func (m *TimeoutTestHandler) Shutdown() {
	m.shutdownReceived = true
}

func TestNewConsumer_CustomTimeouts(t *testing.T) {
	ctx := context.Background()
	awsCfg := loadAWSDefaultConfig(ctx)

	handler := &TimeoutTestHandler{t: t}
	defaultTimeout := 45 * time.Second
	config := Config{
		QueueURL:                 "https://sqs.us-east-1.amazonaws.com/123456789012/test",
		WorkersNum:               1,
		VisibilityTimeoutSeconds: 60,
		BatchSize:                10,
		HandlerTimeoutDuration:   &defaultTimeout,
	}

	consumer, err := NewConsumer(awsCfg, config, handler)
	assert.NoError(t, err)
	assert.Equal(t, defaultTimeout, *consumer.cfg.HandlerTimeoutDuration)
}

func TestConsume_HandlerTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	awsCfg := loadAWSDefaultConfig(ctx)
	queueName := strings.ToLower(t.Name())
	queueUrl := createQueue(t, ctx, awsCfg, queueName)

	defaultTimeout := 2 * time.Second
	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               1,
		VisibilityTimeoutSeconds: 30,
		BatchSize:                1,
		HandlerTimeoutDuration:   &defaultTimeout,
	}

	handler := &TimeoutTestHandler{
		t:               t,
		processingDelay: defaultTimeout + 1*time.Second, // 1 second longer than timeout
	}

	consumer, err := NewConsumer(awsCfg, config, handler)
	assert.NoError(t, err)

	go consumer.Consume(ctx)

	t.Cleanup(func() {
		_, err := consumer.sqs.PurgeQueue(context.Background(), &sqs.PurgeQueueInput{QueueUrl: queueUrl})
		if err != nil {
			t.Logf("failed to purge queue: %v", err)
		}
	})

	// Send a test message
	expectedMsg := TestMsg{Name: "TimeoutTest"}
	sendTestMsg(t, ctx, consumer.sqs, queueUrl, expectedMsg)

	// Wait for processing
	time.Sleep(time.Second * 4)

	// Verify handler was called and timed out
	assert.Equal(t, 1, handler.msgsReceivedCount)
	assert.True(t, handler.timeoutOccurred, "Handler should have timed out")

	// Message should still be deleted despite timeout (because timeout is treated as an error)
	// Wait a second more to ensure delete operation completes
	time.Sleep(time.Second * 1)
	messageCount := getNumOfVisibleMessagesInQueue(t, ctx, consumer.sqs, queueUrl)
	assert.Equal(t, 0, messageCount, "Message should be deleted even after handler timeout")
}

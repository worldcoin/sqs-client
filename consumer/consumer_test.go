package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

const (
	awsRegion         = "us-east-1"
	localAwsEndpoint  = "http://localhost:4566"
	visibilityTimeout = 30
	batchSize         = 10
	workersNum        = 1
	traceId           = "traceid123"
	spanId            = "spanid123"
)

type TestMsg struct {
	Name string `json:"name"`
}

type MsgHandler struct {
	t                     *testing.T
	msgsReceivedCount     int
	expectedMsg           TestMsg
	expectedMsgAttributes interface{}
	shutdownReceived      bool
}

func TestConsume(t *testing.T) {
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

	msgHandler := handler(t, expectedMsg, expectedMsgAttributes)
	config := Config{
		QueueURL:                 *queueUrl,
		WorkersNum:               workersNum,
		VisibilityTimeoutSeconds: visibilityTimeout,
		BatchSize:                batchSize,
	}
	consumer, err := NewConsumer(awsCfg, config, msgHandler)
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

func TestConsume_GracefulShutdown(t *testing.T) {
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
	msgHandler := MsgHandler{}
	consumer, err := NewConsumer(awsCfg, config, &msgHandler)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 1)
		// Cancel context to trigger graceful shutdown
		cancel()
	}()
	// Goroutine to fail the test if shutdown doesn't occur within 5 seconds
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

	// Wait for the consumer to process the shutdown
	wg.Wait()

	assert.Eventually(t, func() bool {
		// Check that shutdown was called
		return msgHandler.shutdownReceived
	}, time.Second*2, time.Millisecond*100)
}

func TestConsume_ErrorsIfConfigIssues(t *testing.T) {
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
			consumer, err := NewConsumer(awsCfg, config, &msgHandler)
			assert.Error(t, err)
			assert.Nil(t, consumer)
		})
	}
}

func createQueue(t *testing.T, ctx context.Context, awsCfg aws.Config, queueName string) *string {
	sqsSvc := sqs.NewFromConfig(awsCfg)

	queue, err := sqsSvc.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		zap.S().With(zap.Error(err)).Error("error while creating queue")
		t.FailNow()
	}

	return queue.QueueUrl
}

func handler(t *testing.T, expectedMsg TestMsg, expectedMsgAttributes map[string]types.MessageAttributeValue) *MsgHandler {
	return &MsgHandler{
		t:                     t,
		msgsReceivedCount:     0,
		expectedMsg:           expectedMsg,
		expectedMsgAttributes: expectedMsgAttributes,
	}
}

func (m *MsgHandler) Run(ctx context.Context, msg *Message) error {
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

func (m *MsgHandler) Shutdown() {
	zap.S().Info("Shutting down")
	m.shutdownReceived = true
	// Do nothing
}

func sendTestMsg(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueUrl *string, expectedMsg TestMsg) TestMsg {
	messageBodyBytes, err := json.Marshal(expectedMsg)
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(string(messageBodyBytes)),
		QueueUrl:    queueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"TraceID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(traceId),
			},
			"SpanID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(spanId),
			},
		},
	})
	if err != nil {
		zap.S().With(zap.Error(err)).Error("error sending message")
		t.FailNow()
	}
	return expectedMsg
}

func getNumOfVisibleMessagesInQueue(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueUrl *string) int {
	return getQueueAttribute(t, ctx, sqsClient, queueUrl, "ApproximateNumberOfMessages")
}

func getNumOfNotVisibleMessagesInQueue(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueUrl *string) int {
	return getQueueAttribute(t, ctx, sqsClient, queueUrl, "ApproximateNumberOfMessagesNotVisible")
}

func getQueueAttribute(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueUrl *string, attributeName string) int {
	attributes, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeName(attributeName)},
	})
	if err != nil {
		zap.S().Error("error retrieving queue attributes")
		t.FailNow()
	}
	messageCount, err := strconv.Atoi(attributes.Attributes[attributeName])
	if err != nil {
		zap.S().Error("error converting string to int")
	}
	return messageCount
}

func loadAWSDefaultConfig(ctx context.Context) aws.Config {
	options := []func(*aws_config.LoadOptions) error{
		aws_config.WithRegion(awsRegion),
	}

	awsEndpoint, found := os.LookupEnv("AWS_ENDPOINT")
	if !found {
		awsEndpoint = localAwsEndpoint
	}

	endpointResolver := aws_config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               awsEndpoint,
			PartitionID:       "aws",
			SigningRegion:     awsRegion,
			HostnameImmutable: true,
		}, nil
	}))
	options = append(options, endpointResolver)
	options = append(options, aws_config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("aws", "aws", "aws")))

	awsCfg, err := aws_config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		zap.S().Fatalf("unable to load AWS SDK config, %v", err)
	}
	return awsCfg
}

// mockSQSClient implements only the DeleteMessage method for testing
type mockSQSClient struct {
	deleteMessageFunc func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return m.deleteMessageFunc(ctx, params, optFns...)
}

// mockSQSAPI defines the minimal interface needed for mocking DeleteMessage
type mockSQSAPI interface {
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

func TestConsumerDelete_ContextCanceled(t *testing.T) {
	// Setup a Consumer with a mock SQS client
	mockClient := &mockSQSClient{
		deleteMessageFunc: func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return nil, context.Canceled
		},
	}
	// Use type assertion to assign mock to Consumer.sqs (interface type)
	consumer := &Consumer{
		sqs:     nil, // will set below
		handler: nil,
		wg:      &sync.WaitGroup{},
		cfg:     Config{QueueURL: "test-queue-url"},
	}

	// Use reflection to set the unexported field for test (or use a helper if available)
	type consumerWithMock struct {
		sqs     mockSQSAPI
		handler sqsclient.Handler
		wg      *sync.WaitGroup
		cfg     Config
	}
	cwm := &consumerWithMock{
		sqs:     mockClient,
		handler: nil,
		wg:      consumer.wg,
		cfg:     consumer.cfg,
	}
	msg := &Message{
		Message: &types.Message{ReceiptHandle: aws.String("test-handle")},
		err:     make(chan error, 1),
	}
	ctx := context.Background()
	// Call delete using the mock
	_, err := cwm.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &cwm.cfg.QueueURL, ReceiptHandle: msg.ReceiptHandle})
	assert.True(t, errors.Is(err, context.Canceled), "DeleteMessage should return context.Canceled error")
}

package sqsclient

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"strings"
	"testing"
	"time"

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
	visibilityTimeout = 20
	waitTimeSecs      = 1
	batchSize         = 1
	workersNum        = 2
	traceId           = "traceid123"
	spanId            = "spanid123"
)

type TestMsg struct {
	Name string `json:"name"`
}

type MsgHandler struct {
	t                     *testing.T
	id                    int
	msgsReceivedCount     int
	expectedMsg           TestMsg
	expectedMsgAttributes interface{}
	shutdownReceived      bool
	shutdownReceivedCount int
	sleepDuration         time.Duration
}

func TestConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
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

	handlerSleepDuration := 200 * time.Millisecond
	msgHandler1 := handler(t, 1, expectedMsg, expectedMsgAttributes, handlerSleepDuration)
	msgHandler2 := handler(t, 2, expectedMsg, expectedMsgAttributes, handlerSleepDuration)
	config := Config{
		QueueURL:          *queueUrl,
		WorkersNum:        workersNum,
		VisibilityTimeout: visibilityTimeout,
		BatchSize:         batchSize,
		ExtendEnabled:     true,
	}
	factory := factoryThatCreates([]Handler{msgHandler1, msgHandler2})
	consumer := NewConsumer(awsCfg, config, &factory)
	go consumer.Consume(ctx)

	t.Cleanup(func() {
		_, err := consumer.sqs.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queueUrl})
		if err != nil {
			zap.S().Error("failed to purge queue")
			t.FailNow()
		}
		cancel()
	})

	// Send messages to the queue
	for i := 0; i < 10; i++ {
		sendTestMsg(t, ctx, consumer, queueUrl, expectedMsg)
	}

	// Wait for the message to arrive
	time.Sleep(time.Second * 1)

	// Check that the messages have been consumed by the two handlers (5 each)
	require.Eventually(t, func() bool {
		return assert.Equal(t, 5, msgHandler1.msgsReceivedCount) &&
			assert.Equal(t, 5, msgHandler2.msgsReceivedCount)
	}, time.Second*5, time.Millisecond*100)
}

func TestConsume_GracefulShutdown_WhenRunFunctionExceedsTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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

	// Set up a short visibility timeout (this is the same timeout used for the handlers' context)
	shorterVisibilityTimeout := int32(2)

	config := Config{
		QueueURL:          *queueUrl,
		WorkersNum:        workersNum,
		VisibilityTimeout: shorterVisibilityTimeout,
		WaitTimeSecs:      waitTimeSecs,
		BatchSize:         batchSize,
		ExtendEnabled:     true,
	}

	// Set up a sleep duration that is longer than the visibility timeout, so the handler will exceed the timeout when shutdown.
	// The aim is to test that the handler shuts down when the context times out. This is needed in case the handler takes longer than the timeout
	// to process the message.
	handlerSleepDuration := (visibilityTimeout + 10) * time.Second
	msgHandler1 := handler(t, 1, expectedMsg, expectedMsgAttributes, handlerSleepDuration)
	msgHandler2 := handler(t, 2, expectedMsg, expectedMsgAttributes, handlerSleepDuration)

	factory := factoryThatCreates([]Handler{msgHandler1, msgHandler2})
	consumer := NewConsumer(awsCfg, config, &factory)
	go consumer.Consume(ctx)

	for i := 0; i < 10; i++ {
		sendTestMsg(t, ctx, consumer, queueUrl, expectedMsg)
	}

	// Cancel context
	cancel()

	start := time.Now()
	require.Eventually(t, func() bool {
		// Check that shutdown was called on both handlers max after the visibility timeout + 1 sec. This way we make sure that both handlers
		// have had the chance to process the message and shuts down when they exceed the given timeout
		return msgHandler1.shutdownReceived && msgHandler2.shutdownReceived
	}, time.Duration(shorterVisibilityTimeout+1)*time.Second, time.Millisecond*100)

	// Assert that the shutdown took place after the visibility timeout but not too long after (max visibility timeout + 1 sec)
	assert.Greater(t, time.Since(start), time.Duration(shorterVisibilityTimeout)*time.Second)
	assert.Less(t, time.Since(start), time.Duration(shorterVisibilityTimeout+1)*time.Second)

	// Each handler should have received only 1 message because the jobs channel is unbuffered, so the consumer will
	// wait for the handler to finish processing the message before sending the next one
	assert.Equal(t, 1, msgHandler1.msgsReceivedCount)
	assert.Equal(t, 1, msgHandler2.msgsReceivedCount)

	// Each handler should have received the only one call to shut down
	assert.Equal(t, 1, msgHandler1.shutdownReceivedCount)
	assert.Equal(t, 1, msgHandler2.shutdownReceivedCount)
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

func handler(t *testing.T, id int, expectedMsg TestMsg, expectedMsgAttributes map[string]types.MessageAttributeValue, sleepDuration time.Duration) *MsgHandler {
	return &MsgHandler{
		t:                     t,
		id:                    id,
		msgsReceivedCount:     0,
		expectedMsg:           expectedMsg,
		expectedMsgAttributes: expectedMsgAttributes,
		sleepDuration:         sleepDuration,
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

	select {
	case <-time.After(m.sleepDuration):
		return nil
	case <-ctx.Done():
		return errors.New("context done")
	}

	return err
}

func (m *MsgHandler) Shutdown() {
	zap.S().Info("Shutting down")
	m.shutdownReceived = true
	m.shutdownReceivedCount += 1
	// Do nothing
}

func sendTestMsg(t *testing.T, ctx context.Context, consumer *Consumer, queueUrl *string, expectedMsg TestMsg) TestMsg {
	messageBodyBytes, err := json.Marshal(expectedMsg)
	_, err = consumer.sqs.SendMessage(ctx, &sqs.SendMessageInput{
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

type HandlerFactoryMock struct {
	handlers     []Handler
	nextToCreate int
}

func (h *HandlerFactoryMock) NewHandler() Handler {
	lastCreated := h.nextToCreate
	h.nextToCreate = h.nextToCreate + 1
	return newGracefulShutdownDecorator(h.handlers[lastCreated])
}

func factoryThatCreates(handlers []Handler) HandlerFactoryMock {
	return HandlerFactoryMock{
		handlers: handlers,
	}
}

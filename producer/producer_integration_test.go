package producer

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ProducerIntegrationTestSuite struct {
	suite.Suite
	client       *sqs.Client
	cleanup      func()
	queueURL     string
	fifoQueueURL string
}

func TestProducerIntegrationSuite(t *testing.T) {
	cfg := loadAWSDefaultConfig(t)
	client := sqs.NewFromConfig(cfg)

	s := new(ProducerIntegrationTestSuite)
	s.client = client

	suite.Run(t, s)
}

func loadAWSDefaultConfig(t *testing.T) aws.Config {
	t.Helper()
	const region = "us-east-1"
	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           endpoint,
				PartitionID:   "aws",
				SigningRegion: region,
			}, nil
		})),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("aws", "aws", "aws")),
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	require.NoError(t, err)
	return cfg
}

func (s *ProducerIntegrationTestSuite) SetupSuite() {
	ctx := context.Background()

	// Create a standard queue
	stdName := strings.ToLower("producer-int-standard")
	out, err := s.client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(stdName)})
	require.NoError(s.T(), err)
	s.queueURL = aws.ToString(out.QueueUrl)

	// Create a FIFO queue
	fifoName := strings.ToLower("producer-int.fifo")
	outFifo, err := s.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(fifoName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(s.T(), err)
	s.fifoQueueURL = aws.ToString(outFifo.QueueUrl)
}

func (s *ProducerIntegrationTestSuite) TearDownTest() {
	ctx := context.Background()
	// Purge both queues between tests
	if s.queueURL != "" {
		_, _ = s.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(s.queueURL)})
	}
	if s.fifoQueueURL != "" {
		_, _ = s.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(s.fifoQueueURL)})
	}
	time.Sleep(500 * time.Millisecond)
}

func (s *ProducerIntegrationTestSuite) TestSendMessage_StandardQueue() {
	ctx := context.Background()
	p := NewProducer(s.client, s.queueURL, false)

	err := p.SendMessageToQueue(ctx, SQSMessage{messageBody: "hello"})
	assert.NoError(s.T(), err)

	// Verify the message is in the queue
	rm, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(rm.Messages))
}

func (s *ProducerIntegrationTestSuite) TestSendMessage_FIFOQueue() {
	ctx := context.Background()
	p := NewProducer(s.client, s.fifoQueueURL, true)

	group := "grp-1"
	dedup := "ddp-1"
	err := p.SendMessageToQueue(ctx, SQSMessage{messageBody: "hello", messageGroupID: &group, messageDeduplicationID: &dedup})
	assert.NoError(s.T(), err)

	// Verify the message is in the queue
	rm, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.fifoQueueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
		AttributeNames:      []types.QueueAttributeName{"MessageGroupId"},
	})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(rm.Messages))
}

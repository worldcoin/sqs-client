package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSendMessageToQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	type mocks struct {
		sqs *MocksqsAPI
	}

	stdQueue := "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
	fifoQueue := "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo"

	dedup := "dedup-1"
	group := "group-1"

	sqsInputStdQueue := &sqs.SendMessageInput{
		MessageBody: aws.String("hello"),
		QueueUrl:    aws.String(stdQueue),
	}

	sqsInputFifoQueue := &sqs.SendMessageInput{
		MessageBody:            aws.String("hello"),
		QueueUrl:               aws.String(fifoQueue),
		MessageDeduplicationId: aws.String(dedup),
		MessageGroupId:         aws.String(group),
	}

	tests := map[string]struct {
		queueURL string
		isFifo   bool
		msg      SQSMessage
		mocks    func(m mocks)
		expErr   error
	}{
		"standard - success": {
			queueURL: stdQueue,
			isFifo:   false,
			msg:      SQSMessage{messageBody: "hello"},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputStdQueue).
					DoAndReturn(func(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
						require.NotNil(t, in)
						assert.Equal(t, stdQueue, aws.ToString(in.QueueUrl))
						assert.Equal(t, "hello", aws.ToString(in.MessageBody))
						assert.Nil(t, in.MessageDeduplicationId)
						assert.Nil(t, in.MessageGroupId)
						return &sqs.SendMessageOutput{}, nil
					})
			},
		},
		"standard - sqs error": {
			queueURL: stdQueue,
			isFifo:   false,
			msg:      SQSMessage{messageBody: "hello"},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputStdQueue).
					Return(nil, errors.New("sqs error"))
			},
			expErr: errors.New("error sending message to queue https://sqs.us-east-1.amazonaws.com/123456789012/test-queue, reason: sqs error"),
		},
		"missing message body - fifo queue": {
			queueURL: fifoQueue,
			isFifo:   true,
			msg:      SQSMessage{messageBody: ""},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: message body cannot be empty"),
		},
		"missing message body - standard queue": {
			queueURL: stdQueue,
			isFifo:   false,
			msg:      SQSMessage{messageBody: ""},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: message body cannot be empty"),
		},
		"fifo - missing message group id": {
			queueURL: fifoQueue,
			isFifo:   true,
			msg:      SQSMessage{messageBody: "payload"},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: fifo queue requires MessageGroupId"),
		},
		"standard - fifo fields set": {
			queueURL: stdQueue,
			isFifo:   false,
			msg:      SQSMessage{messageBody: "payload", messageDeduplicationID: &dedup, messageGroupID: &group},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: fifo fields set for a standard queue"),
		},
		"fifo - success with group and dedup": {
			queueURL: fifoQueue,
			isFifo:   true,
			msg:      SQSMessage{messageBody: "hello", messageDeduplicationID: &dedup, messageGroupID: &group},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputFifoQueue).
					DoAndReturn(func(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
						require.NotNil(t, in)
						assert.Equal(t, fifoQueue, aws.ToString(in.QueueUrl))
						assert.Equal(t, "hello", aws.ToString(in.MessageBody))
						assert.Equal(t, dedup, aws.ToString(in.MessageDeduplicationId))
						assert.Equal(t, group, aws.ToString(in.MessageGroupId))
						return &sqs.SendMessageOutput{}, nil
					})
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := mocks{sqs: NewMocksqsAPI(ctrl)}
			tt.mocks(m)

			var p *Producer
			if tt.isFifo {
				p = NewProducerFIFO(m.sqs, tt.queueURL)
			} else {
				p = NewProducerStandard(m.sqs, tt.queueURL)
			}

			err := p.SendMessageToQueue(ctx, tt.msg)
			if tt.expErr != nil {
				assert.EqualError(t, err, tt.expErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

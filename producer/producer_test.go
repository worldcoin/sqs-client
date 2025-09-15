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

	queueStd := "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
	queueFIFO := "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo"

	dedup := "dedup-1"
	group := "group-1"

	sqsInputStdQueue := &sqs.SendMessageInput{
		MessageBody: aws.String("hello"),
		QueueUrl:    aws.String(queueStd),
	}

	sqsInputFifoQueue := &sqs.SendMessageInput{
		MessageBody:            aws.String("hello"),
		QueueUrl:               aws.String(queueFIFO),
		MessageDeduplicationId: aws.String(dedup),
		MessageGroupId:         aws.String(group),
	}

	tests := map[string]struct {
		queueURL string
		isFIFO   bool
		msg      SQSMessage
		mocks    func(m mocks)
		expErr   error
	}{
		"standard - success": {
			queueURL: queueStd,
			isFIFO:   false,
			msg:      SQSMessage{MessageBody: "hello"},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputStdQueue).
					DoAndReturn(func(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
						require.NotNil(t, in)
						assert.Equal(t, queueStd, aws.ToString(in.QueueUrl))
						assert.Equal(t, "hello", aws.ToString(in.MessageBody))
						assert.Nil(t, in.MessageDeduplicationId)
						assert.Nil(t, in.MessageGroupId)
						return &sqs.SendMessageOutput{}, nil
					})
			},
		},
		"standard - sqs error": {
			queueURL: queueStd,
			isFIFO:   false,
			msg:      SQSMessage{MessageBody: "hello"},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputStdQueue).
					Return(nil, errors.New("sqs error"))
			},
			expErr: errors.New("error sending message to queue https://sqs.us-east-1.amazonaws.com/123456789012/test-queue, reason: sqs error"),
		},
		"missing message body - FIFO queue": {
			queueURL: queueFIFO,
			isFIFO:   true,
			msg:      SQSMessage{MessageBody: ""},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: message body cannot be empty"),
		},
		"missing message body - standard queue": {
			queueURL: queueStd,
			isFIFO:   false,
			msg:      SQSMessage{MessageBody: ""},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: message body cannot be empty"),
		},
		"FIFO - missing message group id": {
			queueURL: queueFIFO,
			isFIFO:   true,
			msg:      SQSMessage{MessageBody: "payload"},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: FIFO queue requires MessageGroupId"),
		},
		"standard - FIFO fields set": {
			queueURL: queueStd,
			isFIFO:   false,
			msg:      SQSMessage{MessageBody: "payload", MessageDeduplicationID: &dedup, MessageGroupID: &group},
			mocks:    func(_ mocks) {},
			expErr:   errors.New("invalid sqs message: FIFO fields set for a standard queue"),
		},
		"FIFO - success with group and dedup": {
			queueURL: queueFIFO,
			isFIFO:   true,
			msg:      SQSMessage{MessageBody: "hello", MessageDeduplicationID: &dedup, MessageGroupID: &group},
			mocks: func(m mocks) {
				m.sqs.
					EXPECT().
					SendMessage(gomock.Any(), sqsInputFifoQueue).
					DoAndReturn(func(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
						require.NotNil(t, in)
						assert.Equal(t, queueFIFO, aws.ToString(in.QueueUrl))
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
			if tt.isFIFO {
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

package producer

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type sqsAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Producer struct provides method to send messages to an SQS queue.
type Producer struct {
	sqsAPI   sqsAPI
	queueURL string
	isFifo   bool
}

// NewProducer constructor of Producer.
func NewProducer(sqsAPI sqsAPI, queueURL string, isFifo bool) *Producer {
	return &Producer{
		sqsAPI:   sqsAPI,
		queueURL: queueURL,
		isFifo:   isFifo,
	}
}

// SQSMessage struct to encapsulate all the parameters needed to construct an sqs message to be sent by the producer.
// It can be used for sending messages for both fifo and standard queues.
// In case of standard messageDeduplicationID is going to be nil.
type SQSMessage struct {
	messageBody            string
	messageDeduplicationID *string
	messageGroupID         *string
}

func (m *SQSMessage) toSQSInput() *sqs.SendMessageInput {
	res := sqs.SendMessageInput{
		MessageBody: &m.messageBody,
	}
	if m.messageDeduplicationID != nil {
		res.MessageDeduplicationId = m.messageDeduplicationID
	}
	if m.messageGroupID != nil {
		res.MessageGroupId = m.messageGroupID
	}

	return &res
}

// SendMessageToQueue receives the message as parameter and validates it,
// constructs sqs input and sends the message to the configured queue url.
func (p *Producer) SendMessageToQueue(ctx context.Context, msg SQSMessage) error {
	err := validateSQSMessage(msg, p.isFifo)
	if err != nil {
		return fmt.Errorf("invalid sqs message: %w", err)
	}

	input := msg.toSQSInput()
	input.QueueUrl = &p.queueURL
	_, err = p.sqsAPI.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("error sending message to queue %s, reason: %w", p.queueURL, err)
	}

	return nil
}

func validateSQSMessage(msg SQSMessage, isFifo bool) error {
	if msg.messageBody == "" {
		return fmt.Errorf("message body cannot be empty")
	}
	if isFifo {
		// validation for fifo queues
		if msg.messageGroupID == nil || *msg.messageGroupID == "" {
			return errors.New("fifo queue requires MessageGroupId")
		}
	} else {
		// validation for standard queues
		if msg.messageGroupID != nil || msg.messageDeduplicationID != nil {
			return errors.New("fifo fields set for a standard queue")
		}
	}
	return nil
}

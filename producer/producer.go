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
	isFIFO   bool
}

// NewProducerStandard constructor of Producer for standard queues.
func NewProducerStandard(sqsAPI sqsAPI, queueURL string) *Producer {
	return &Producer{
		sqsAPI:   sqsAPI,
		queueURL: queueURL,
		isFIFO:   false,
	}
}

// NewProducerFIFO constructor of Producer for FIFO queues.
func NewProducerFIFO(sqsAPI sqsAPI, queueURL string) *Producer {
	return &Producer{
		sqsAPI:   sqsAPI,
		queueURL: queueURL,
		isFIFO:   true,
	}
}

// SQSMessage struct to encapsulate all the parameters needed to construct an sqs message to be sent by the producer.
// It can be used for sending messages for both FIFO and standard queues.
// In case of standard messageDeduplicationID is going to be nil.
type SQSMessage struct {
	MessageBody            string
	MessageDeduplicationID *string
	MessageGroupID         *string
}

func (m *SQSMessage) toSQSInput() *sqs.SendMessageInput {
	res := sqs.SendMessageInput{
		MessageBody: &m.MessageBody,
	}
	if m.MessageDeduplicationID != nil {
		res.MessageDeduplicationId = m.MessageDeduplicationID
	}
	if m.MessageGroupID != nil {
		res.MessageGroupId = m.MessageGroupID
	}

	return &res
}

// SendMessageToQueue receives the message as parameter and validates it,
// constructs sqs input and sends the message to the configured queue url.
func (p *Producer) SendMessageToQueue(ctx context.Context, msg SQSMessage) error {
	err := validateSQSMessage(msg, p.isFIFO)
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

func validateSQSMessage(msg SQSMessage, isFIFO bool) error {
	if msg.MessageBody == "" {
		return fmt.Errorf("message body cannot be empty")
	}
	if isFIFO {
		// validation for FIFO queues
		if msg.MessageGroupID == nil || *msg.MessageGroupID == "" {
			return errors.New("FIFO queue requires MessageGroupId")
		}
	} else {
		// validation for standard queues
		if msg.MessageGroupID != nil || msg.MessageDeduplicationID != nil {
			return errors.New("FIFO fields set for a standard queue")
		}
	}
	return nil
}

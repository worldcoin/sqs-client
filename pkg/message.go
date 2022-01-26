package sqs

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	*sqs.Message
	err chan error
}

func (m *Message) Decode(out interface{}) error {
	return json.Unmarshal(m.body(), &out)
}

func newMessage(m *sqs.Message) *Message {
	return &Message{m, make(chan error, 1)}
}

func (m *Message) body() []byte {
	return []byte(*m.Message.Body)
}

func (m *Message) ErrorResponse(err error) error {
	go func() {
		m.err <- err
	}()
	return err
}

func (m *Message) Success() {
	go func() {
		m.err <- nil
	}()
}

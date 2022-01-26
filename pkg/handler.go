package sqs

import (
    "context"
)

type Handler func(ctx context.Context, m *Message) error
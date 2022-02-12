package sqsclient

import "context"

type Handler interface {
	Run(ctx context.Context, msg *Message) error
}

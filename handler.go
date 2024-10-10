package sqsclient

import "context"

type Handler interface {
	Run(ctx context.Context, msg *Message) error
	Shutdown()
}

type HandlerWithIdleTrigger interface {
	Handler
	IdleTimeout(ctx context.Context) error
}

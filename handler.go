package sqsclient

import (
	"context"
	"go.uber.org/zap"
	"sync"
)

type Handler interface {
	Run(ctx context.Context, msg *Message) error
	Shutdown()
}

type HandlerFactory interface {
	NewHandler() Handler
}

type GracefulShutdownDecorator struct {
	handler Handler
	mu      *sync.Mutex
}

// NewGracefulShutdownDecorator returns a new instance of GracefulShutdownDecorator. This decorator is used to ensure that the current message is processed before shutting down the handler.
// We decided to implement this decorator because the sqs-client does not provide a way to wait for the current message to finish processing before shutting down the handler.
// The handler is guaranteed a timeout to finish processing the current message before the shutdown is complete.
func newGracefulShutdownDecorator(handler Handler) *GracefulShutdownDecorator {
	return &GracefulShutdownDecorator{
		handler: handler,
		mu:      &sync.Mutex{},
	}
}

func (d *GracefulShutdownDecorator) Shutdown() {
	// Wait for the current message to finish processing. The Run method releases the mutex when it completes, so if this mutex is locked, the message is still being processed.
	// If the mutex is not locked, the last message has already been processed and no new messages will come through because the sqs-client only receives new messages if the ctx is not done.
	zap.S().Info("acquiring lock to shut down handler")
	d.mu.Lock()
	zap.S().Info("lock acquired, shutting down handler")
	d.handler.Shutdown()
	defer d.mu.Unlock()
}

func (d *GracefulShutdownDecorator) Run(ctx context.Context, msg *Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.handler.Run(ctx, msg)
}

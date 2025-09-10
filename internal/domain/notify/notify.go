package notify

import "context"

type Notification struct {
	Channel string
	Payload string
	Raw     any
}

type Notifier interface {
	Connect(ctx context.Context) error
	Listen(ctx context.Context, channels ...string) (<-chan Notification, <-chan error, error)
	Close() error
}

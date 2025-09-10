package pgnotify

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"db_listener/internal/domain/notify"
	"github.com/jackc/pgx/v5"
)

type PgNotifier struct {
	conn      *pgx.Conn
	dsn       string
	channels  []string
	listening atomic.Bool
}

func New(dsn string) *PgNotifier {
	return &PgNotifier{dsn: dsn}
}

func (n *PgNotifier) Connect(ctx context.Context) error {
	c, err := pgx.Connect(ctx, n.dsn)
	if err != nil {
		return err
	}
	n.conn = c
	return nil
}

var channelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateChannel — простая защита от инъекций в LISTEN.
func validateChannel(ch string) error {
	if !channelNameRe.MatchString(ch) {
		return fmt.Errorf("invalid channel name: %q", ch)
	}
	return nil
}

func (p *PgNotifier) listenOnce(ctx context.Context, channels ...string) error {
	for _, ch := range channels {
		if err := validateChannel(ch); err != nil {
			return err
		}
		if _, err := p.conn.Exec(ctx, "UNLISTEN "+ch); err != nil {
			// игнорируем ошибку: могли быть не подписаны
		}
		if _, err := p.conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return fmt.Errorf("LISTEN %s: %w", ch, err)
		}
	}
	return nil
}

func (p *PgNotifier) resubscribe(ctx context.Context) error {
	if len(p.channels) == 0 {
		return nil
	}
	if _, err := p.conn.Exec(ctx, "UNLISTEN *"); err != nil {
		return err
	}
	return p.listenOnce(ctx, p.channels...)
}

func (p *PgNotifier) reconnect(ctx context.Context) error {
	if err := p.Close(); err != nil {
		return err
	}
	return p.Connect(ctx)
}

func (p *PgNotifier) Listen(ctx context.Context, channels ...string) (<-chan notify.Notification, <-chan error, error) {
	if !p.listening.CompareAndSwap(false, true) {
		return nil, nil, errors.New("pgnotify: Listen already running on this instance")
	}
	if p.conn == nil {
		return nil, nil, errors.New("pgnotify: nil connection")
	}

	if err := p.listenOnce(ctx, channels...); err != nil {
		return nil, nil, err
	}
	p.channels = append([]string(nil), channels...)

	out := make(chan notify.Notification, 128)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)
		defer p.listening.Store(false)

		for {
			ntf, err := p.conn.WaitForNotification(ctx)
			if err != nil {
				// штатное завершение
				if ctx.Err() != nil {
					return
				}
				// пробуем восстановиться
				errc <- err
				backoff := time.Second
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
						if rErr := p.reconnect(ctx); rErr != nil {
							errc <- fmt.Errorf("reconnect failed: %w", rErr)
						} else if rsErr := p.resubscribe(ctx); rsErr != nil {
							errc <- fmt.Errorf("resubscribe failed: %w", rsErr)
						} else {
							goto CONTINUE
						}
						if backoff < 10*time.Second {
							backoff *= 2
						}
					}
				}
			}
		CONTINUE:
			out <- notify.Notification{
				Channel: ntf.Channel,
				Payload: ntf.Payload,
				Raw:     ntf,
			}
		}
	}()

	return out, errc, nil
}

func (p *PgNotifier) Close() error {
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}

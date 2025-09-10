package listener

import (
	"context"
	"db_listener/internal/config"
	"db_listener/internal/domain/notify"
	"log"
)

type Notifier interface {
	Listen(ctx context.Context, channels ...string) (<-chan notify.Notification, <-chan error, error)
}

type Service struct {
	notifier Notifier
	cfg      *config.Config
}

func New(notifier Notifier, cfg *config.Config) *Service {
	return &Service{
		notifier: notifier,
		cfg:      cfg,
	}
}

// Run запускает прослушивание
func (s *Service) Run(ctx context.Context, channels ...string) error {
	msgs, errs, err := s.notifier.Listen(ctx, channels...)
	if err != nil {
		return err
	}

	log.Printf("Start listening channels: %v", channels)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-errs:
			// Можно заменить на метрики/алертинг/ретраи.
			log.Printf("listener error: %v", e)
		case m, ok := <-msgs:
			if !ok {
				return nil
			}
			var getMessage []byte
			getMessage, err := GetMessage(m.Payload, s.cfg)
			if err != nil {
				log.Printf("error receiving data for letter: %v", err)
			} else {
				err = SendToKafka(context.Background(), s.cfg, getMessage)
				if err != nil {
					log.Printf("error sending data for kafka: %v", err)
				} else {
					err = readKafka(context.Background(), s.cfg)
					if err != nil {
						log.Printf("error reading kafka or sending email for kafka: %v", err)
					} else {
						log.Printf("successfully sended email")
					}
				}
			}
		}
	}
}

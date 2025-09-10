package app

import (
	"context"
	"db_listener/internal/adapters/logger"
	"db_listener/internal/adapters/pgnotify"
	"fmt"

	"db_listener/internal/config"
	"db_listener/internal/domain/log"
	"db_listener/internal/domain/notify"
	"db_listener/internal/services/listener"
)

type App struct {
	Listener *listener.Service
	Channels []string
	Close    func() error
	Logger   log.Logger
}

func Build(ctx context.Context, cfg *config.Config) (*App, error) {
	var (
		n   notify.Notifier
		err error
	)

	dsn, dsnErr := cfg.Db.DSN()
	if dsnErr != nil {
		return nil, fmt.Errorf("invalid db config: %w", dsnErr)
	}

	switch cfg.Db.Driver {
	case "postgres":
		n = pgnotify.New(dsn)
		err = n.Connect(ctx)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Db.Driver)
	}
	if err != nil {
		return nil, err
	}

	myLogger, err := logger.New(
		cfg.Logger.GRPCAddress,
		cfg.Logger.FallbackPath,
		cfg.Logger.ServiceName,
	)

	if err != nil {
		return nil, err
	}

	svc := listener.New(n, cfg)

	return &App{
		Listener: svc,
		Channels: cfg.Db.NotifyChannels,
		Close:    n.Close,
		Logger:   myLogger,
	}, nil
}

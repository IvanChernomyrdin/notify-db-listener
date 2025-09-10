package main

import (
	"context"
	"db_listener/internal/app"
	"db_listener/internal/config"
	dLog "db_listener/internal/domain/log"
	"fmt"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.MustLoad()

	a, err := app.Build(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = a.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := a.Listener.Run(ctx, a.Channels...); err != nil && ctx.Err() == nil {
			a.Logger.Error("listener stopped", dLog.Field{Key: "err", Value: err})
		}
	}()

	<-ctx.Done()
	fmt.Println("shutting down...")
}

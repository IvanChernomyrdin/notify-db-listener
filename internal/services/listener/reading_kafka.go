package listener

import (
	"context"
	"log"
	"sync"

	"db_listener/internal/config"

	"github.com/segmentio/kafka-go"
)

func readKafka(ctx context.Context, cfg *config.Config) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	const workerCount = 10 // число горутин для параллельной обработки

	msgCh := make(chan []byte, 100)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Println("Kafka reader shutting down")
					close(msgCh)
					return
				}
				log.Println("Read error:", err)
				continue
			}
			msgCh <- m.Value
		}
	}()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for msg := range msgCh {
				//Распаковка json, формирование и отправка письма
				err := sendMessage(msg)
				if err != nil {
					log.Println("failed to send mail:", err)
				}
			}
		}(i)
	}

	<-ctx.Done()
	close(msgCh)
	wg.Wait()
	return nil
}

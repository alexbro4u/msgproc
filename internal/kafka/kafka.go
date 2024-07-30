package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"msgproc/internal/lib/logger/sl"
)

type Sender struct {
	producer sarama.SyncProducer
	topic    string
	log      *slog.Logger
}

func NewKafkaSender(brokers []string, topic string, log *slog.Logger) (*Sender, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Sender{
		producer: producer,
		topic:    topic,
		log:      log,
	}, nil
}

func (k *Sender) SendMsg(ctx context.Context, msg string, msgID int64) error {
	const op = "services.kafka.SendMsg"

	log := k.log.With(
		slog.String("op", op),
		slog.Int64("msgID", msgID),
	)
	select {
	case <-ctx.Done():
		err := ctx.Err()
		log.Error("context cancelled", sl.Err(err))
		return fmt.Errorf("%s: %w", op, err)
	default:
	}

	message := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(msg),
	}

	partition, offset, err := k.producer.SendMessage(message)
	if err != nil {
		log.Error("failed to send message to kafka", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	log.Info("message sent to kafka",
		slog.Int("partition", int(partition)),
		slog.Int64("offset", offset),
	)

	return nil
}

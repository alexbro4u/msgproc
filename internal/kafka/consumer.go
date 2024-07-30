package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"msgproc/internal/lib/logger/sl"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func Consumer(ctx context.Context, brokerList []string, groupID string, topics []string, handler sarama.ConsumerGroupHandler, log *slog.Logger) error {
	const op = "kafka.Consumer"

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokerList, groupID, config)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.Error("Error from consumer", sl.Err(err))
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Info("context cancelled")
	case <-sigterm:
		log.Info("received termination signal")
	}

	if err = consumerGroup.Close(); err != nil {
		log.Error("Error closing client", sl.Err(err))
	}

	return nil
}

type MessageHandler struct {
	logger *slog.Logger
}

func NewMessageHandler(logger *slog.Logger) *MessageHandler {
	return &MessageHandler{logger: logger}
}

func (h *MessageHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *MessageHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *MessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("Message claimed",
			slog.String("value", string(message.Value)),
			slog.Time("timestamp", message.Timestamp),
			slog.String("topic", message.Topic))
		// Process the message here
		session.MarkMessage(message, "")
	}
	return nil
}

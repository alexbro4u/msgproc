package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"msgproc/internal/lib/logger/sl"
	"strings"
)

type MsgReceiver interface {
	ProcessMessages(ctx context.Context, msgUpdater MessageUpdater) error
}

type MessageUpdater interface {
	UpdateMsgStatus(ctx context.Context, msgID int64, status string) error
	UpdateMsg(ctx context.Context, msgID int64, msg string) error
}

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

	messageContent := map[string]interface{}{
		"msg":   msg,
		"msgID": msgID,
	}
	messageBytes, err := json.Marshal(messageContent)
	if err != nil {
		log.Error("failed to marshal message", sl.Err(err))
		return fmt.Errorf("%s: %w", op, err)
	}

	message := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(messageBytes),
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

type Receiver struct {
	consumerGroup sarama.ConsumerGroup
	topic         string
	log           *slog.Logger
}

func NewKafkaReceiver(log *slog.Logger, brokers []string, topic string, groupID string) (*Receiver, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Version = sarama.V0_10_2_0

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &Receiver{
		consumerGroup: consumerGroup,
		topic:         topic,
		log:           log,
	}, nil
}

func (k *Receiver) ProcessMessages(ctx context.Context, msgUpdater MessageUpdater) error {
	const op = "services.kafka.ProcessMessages"

	handler := &consumerGroupHandler{
		receiver:   k,
		msgUpdater: msgUpdater,
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := k.consumerGroup.Consume(ctx, []string{k.topic}, handler); err != nil {
			k.log.Error("failed to consume messages", sl.Err(err))
			return fmt.Errorf("%s: %w", op, err)
		}
	}
}

type consumerGroupHandler struct {
	receiver   *Receiver
	msgUpdater MessageUpdater
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log := h.receiver.log.With(slog.String("op", "services.kafka.ConsumeClaim"))

	for {
		select {
		case msg := <-claim.Messages():

			select {
			case <-session.Context().Done():
				return session.Context().Err()
			default:
			}

			log.Info("message received from kafka",
				slog.String("msg", string(msg.Value)),
				slog.Int64("offset", msg.Offset),
			)

			var messageContent map[string]interface{}
			err := json.Unmarshal(msg.Value, &messageContent)
			if err != nil {
				log.Error("failed to unmarshal message", sl.Err(err))
				continue
			}

			msgStr, ok := messageContent["msg"].(string)
			if !ok {
				log.Error("message content missing 'msg' field")
				continue
			}

			msgIDFloat, ok := messageContent["msgID"].(float64)
			if !ok {
				log.Error("message content missing 'msgID' field")
				continue
			}
			msgID := int64(msgIDFloat)

			log.Info("processing message",
				slog.String("msg", msgStr),
				slog.Int64("msgID", msgID),
			)

			trimmedMsg := strings.TrimSpace(msgStr)

			err = h.msgUpdater.UpdateMsg(session.Context(), msgID, trimmedMsg)
			if err != nil {
				log.Error("failed to update message", sl.Err(err))

				err = h.msgUpdater.UpdateMsgStatus(session.Context(), msgID, "failed")
				if err != nil {
					log.Error("failed to update message status after processing", sl.Err(err))
					continue
				}

				continue
			}

			err = h.msgUpdater.UpdateMsgStatus(session.Context(), msgID, "completed")
			if err != nil {
				log.Error("failed to update message status after processing", sl.Err(err))
				continue
			}

			session.MarkMessage(msg, "")
			log.Info("message processed",
				slog.Int64("msgID", msgID),
			)
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

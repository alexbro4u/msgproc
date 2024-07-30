package kafka

import (
	"github.com/IBM/sarama"
)

var producer sarama.SyncProducer

func InitProducer(brokerList []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	var err error
	producer, err = sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return err
	}
	return nil
}

func SendMessage(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	return err
}

func CloseProducer() error {
	return producer.Close()
}

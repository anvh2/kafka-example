package producer

import (
	"encoding/json"
	"errors"

	"github.com/Shopify/sarama"
)

type message struct {
	topic   string
	message string
}

// KafkaProducer -
type KafkaProducer struct {
	producer sarama.SyncProducer
}

// NewKafkaProducer -
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		producer: producer,
	}, nil
}

// Close -
func (p *KafkaProducer) Close() {
	p.producer.Close()
}

// SendMessage ...
func (p *KafkaProducer) SendMessage(topic string, message string) error {
	if topic == "" || message == "" {
		return errors.New("Empty topic or message")
	}

	json, err := json.Marshal(message)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(json)),
	}

	_, _, err = p.producer.SendMessage(producerMessage)
	return err
}

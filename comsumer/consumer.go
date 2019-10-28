package comsumer

import (
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

type process func(message *sarama.ConsumerMessage)

// KafkaConsumer -
type KafkaConsumer struct {
	brokers  []string
	topics   []string
	consumer sarama.Consumer
	process  process
}

// NewKafkaConsumer -
func NewKafkaConsumer(brokers []string, topic []string, process process) (*KafkaConsumer, error) {
	config := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		brokers:  brokers,
		topics:   topic,
		consumer: consumer,
		process:  process,
	}, nil
}

// Start -
func (ks *KafkaConsumer) Start() {
	defer ks.consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		for {
			select {
			case <-signals:
				return
			}
		}
	}()
}

// Close -
func (ks *KafkaConsumer) Close() {
	ks.consumer.Close()
}

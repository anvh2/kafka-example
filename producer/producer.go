package producer

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type message struct {
	topic   string
	message string
}

// KafkaProducer -
type KafkaProducer struct {
	producer       sarama.SyncProducer
	done           chan struct{}
	MessageChannel chan message
}

// NewKafkaProducer -
func NewKafkaProducer(brokers []string, partitioner sarama.PartitionerConstructor) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		producer:       producer,
		done:           make(chan struct{}),
		MessageChannel: make(chan message, 100),
	}, nil
}

// Start -
func (kp *KafkaProducer) Start() {
	go func() {
		for {
			select {
			case data := <-kp.MessageChannel:
				kp.sendMessage(data)
			case <-kp.done:
				kp.producer.Close()
				return
			}
		}
	}()
}

// Close -
func (kp *KafkaProducer) Close() {
	kp.producer.Close()
}

func (kp *KafkaProducer) sendMessage(msg message) error {
	json, err := json.Marshal(msg.message)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: msg.topic,
		Value: sarama.StringEncoder(string(json)),
	}

	_, _, err = kp.producer.SendMessage(producerMessage)
	return err
}

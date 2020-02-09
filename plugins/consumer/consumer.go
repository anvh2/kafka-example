package consumer

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type process func(message *sarama.ConsumerMessage)

// KafkaConsumer -
type KafkaConsumer struct {
	brokers    []string
	topic      string
	consumer   sarama.Consumer
	partitions []int32
	messages   chan *sarama.ConsumerMessage
	process    process
	wait       *sync.WaitGroup
	quit       chan struct{}
}

// NewKafkaConsumer -
func NewKafkaConsumer(brokers []string, topic string, process process) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		brokers:    brokers,
		topic:      topic,
		consumer:   consumer,
		partitions: partitions,
		messages:   make(chan *sarama.ConsumerMessage, 5000),
		process:    process,
		wait:       &sync.WaitGroup{},
		quit:       make(chan struct{}),
	}, nil
}

// Start -
func (c *KafkaConsumer) Start() {
	for _, partition := range c.partitions {
		pc, err := c.consumer.ConsumePartition(c.topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatal("Failed to start comsumer for partition", err)
		}

		// close consumer
		go func(pc sarama.PartitionConsumer) {
			<-c.quit
			pc.AsyncClose()
		}(pc)

		// consume message
		c.wait.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer c.wait.Done()
			for message := range pc.Messages() {
				c.messages <- message
			}
		}(pc)

		// process message
		go func() {
			select {
			case message := <-c.messages:
				c.process(message)
				log.Printf("Partition:\t%d\n", message.Partition)
				log.Printf("Offset:\t%d\n", message.Offset)
				log.Printf("Key:\t%s\n", string(message.Key))
				log.Printf("Value:\t%s\n", string(message.Value))
				log.Println()
			case <-c.quit:
				return
			}
		}()
	}

	c.wait.Wait()
}

// Stop -
func (c *KafkaConsumer) Stop() {
	close(c.quit)
	log.Println("Quit consume")
	close(c.messages)
	if err := c.consumer.Close(); err != nil {
		log.Println("Failed to close consumer")
	}
}

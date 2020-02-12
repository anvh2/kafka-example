package consumer

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type process func(message *sarama.ConsumerMessage)

// Consumer -
type Consumer struct {
	brokers    []string
	topic      string
	consumer   sarama.Consumer
	partitions []int32
	messages   chan *sarama.ConsumerMessage
	process    process
	wait       *sync.WaitGroup
	quit       chan struct{}
	numWorker  int32
}

// NewConsumer -
func NewConsumer(brokers []string, topic string, process process, numWorker int32) (*Consumer, error) {
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
	log.Println("PARTITIONS:", partitions)

	return &Consumer{
		brokers:    brokers,
		topic:      topic,
		consumer:   consumer,
		partitions: partitions,
		messages:   make(chan *sarama.ConsumerMessage, 5000),
		process:    process,
		wait:       &sync.WaitGroup{},
		quit:       make(chan struct{}),
		numWorker:  numWorker,
	}, nil
}

// Start -
func (c *Consumer) Start() {
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
		for i := int32(0); i < c.numWorker; i++ {
			c.wait.Add(1)
			go func(idx int32) {
				defer c.wait.Done()
				for {
					select {
					case message := <-c.messages:
						c.process(message)
						log.Printf("Worker:\t%d\n", idx)
						log.Printf("Partition:\t%d\n", message.Partition)
						log.Printf("Offset:\t%d\n", message.Offset)
						log.Printf("Key:\t%s\n", string(message.Key))
						log.Printf("Value:\t%s\n", string(message.Value))
						log.Println()
					case <-c.quit:
						log.Println("Quit process message")
						return
					}
				}
			}(i)
		}
	}

	c.wait.Wait()
}

// Stop -
func (c *Consumer) Stop() {
	close(c.quit)
	log.Println("Quit consume")
	close(c.messages)
	if err := c.consumer.Close(); err != nil {
		log.Println("Failed to close consumer")
	}
}

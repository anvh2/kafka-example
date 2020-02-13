package group

import (
	"context"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type process func(*sarama.ConsumerMessage)

type ConsumerGroup struct {
	borkers      []string
	topic        []string
	groupID      string
	consumer     sarama.ConsumerGroup
	process      process
	ready        chan bool
	waitConsumer *sync.WaitGroup
}

func NewConsumerGroup(brokers []string, topic []string, groupID string, process process) (*ConsumerGroup, error) {
	config := sarama.NewConfig()

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}
	return &ConsumerGroup{
		borkers:      brokers,
		topic:        topic,
		groupID:      groupID,
		consumer:     client,
		process:      process,
		ready:        make(chan bool),
		waitConsumer: &sync.WaitGroup{},
	}, nil
}

func (c *ConsumerGroup) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.waitConsumer.Add(1)
	go func() {
		defer c.waitConsumer.Done()
		for {
			if err := c.consumer.Consume(ctx, c.topic, c); err != nil {
				log.Fatal(err)
			}

			if ctx.Err() != nil {
				return
			}
			c.ready <- true
		}
	}()

	<-c.ready
	c.waitConsumer.Wait()
	cancel()
}

func (c *ConsumerGroup) Stop() {
	c.consumer.Close()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}

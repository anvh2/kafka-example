package echo

import (
	"fmt"
	"log"
	"time"

	"github.com/anvh2/kafka-example/plugins/producer"
	"github.com/spf13/viper"
)

type Server struct {
	producer *producer.Producer
}

// NewServer ...
func NewServer() *Server {
	producer, err := producer.NewProducer(viper.GetStringSlice("kafka.brokers"))
	if err != nil {
		log.Fatal(err)
	}
	return &Server{
		producer: producer,
	}
}

// Run ...
func (s *Server) Run() {
	defer s.producer.Close()
	for {
		time.Sleep(2 * time.Second)
		fmt.Println("Push message")
		err := s.producer.SendMessage(viper.GetString("kafka.topic"), "Hello world")
		if err != nil {
			log.Println(err)
		}
	}
}

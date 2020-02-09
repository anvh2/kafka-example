package echo

import (
	"log"
	"time"

	"github.com/anvh2/kafka-example/plugins/producer"
	"github.com/spf13/viper"
)

type Server struct {
	producer *producer.KafkaProducer
}

// NewServer ...
func NewServer() *Server {
	producer, err := producer.NewKafkaProducer(viper.GetStringSlice("kafka.brokers"))
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
		s.producer.SendMessage(viper.GetString("kafka.topic"), "Hello world")
	}
}

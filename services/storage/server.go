package storage

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/anvh2/kafka-example/plugins/consumer"
	"github.com/spf13/viper"
)

type Server struct {
	consumer *consumer.Consumer
}

// NewServer ...
func NewServer() *Server {
	server := &Server{}
	var err error
	server.consumer, err = consumer.NewConsumer(viper.GetStringSlice("kafka.brokers"),
		viper.GetString("kafka.topic"), server.handleConsumerMessage, int32(8))
	if err != nil {
		log.Fatal(err)
	}
	return server
}

// Run ...
func (s *Server) Run() {
	log.Println("Starting server ...")
	go s.consumer.Start()

	sig := make(chan os.Signal, 1)
	done := make(chan bool)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Server now listening")

	go func() {
		<-sig
		log.Println("Shutdown server")
		s.consumer.Stop()
		done <- true
	}()

	fmt.Println("Ctrl-C to interrupt...")
	<-done
	fmt.Println("Exiting...")
}

func (s *Server) handleConsumerMessage(message *sarama.ConsumerMessage) {
}

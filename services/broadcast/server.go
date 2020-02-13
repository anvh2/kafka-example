package broadcast

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	group "github.com/anvh2/kafka-example/plugins/consumer-group"
	"github.com/spf13/viper"
)

type Server struct {
	consumers *group.ConsumerGroup
}

// NewServer ...
func NewServer() *Server {
	server := &Server{}
	var err error
	server.consumers, err = group.NewConsumerGroup(viper.GetStringSlice("kafka.brokers"),
		[]string{viper.GetString("kafka.topic")}, "broadcast-comsumer-group", server.handleConsumerMessage)
	if err != nil {
		log.Fatal(err)
	}
	return server
}

// Run ...
func (s *Server) Run() {
	log.Println("Starting server ...")
	go s.consumers.Start()

	sig := make(chan os.Signal, 1)
	done := make(chan bool)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Server now listening")

	go func() {
		<-sig
		log.Println("Shutdown server")
		s.consumers.Stop()
		done <- true
	}()

	fmt.Println("Ctrl-C to interrupt...")
	<-done
	fmt.Println("Exiting...")
}

func (s *Server) handleConsumerMessage(message *sarama.ConsumerMessage) {
}

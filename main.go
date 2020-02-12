package main

import (
	"fmt"
	"time"
)

// import "github.com/anvh2/kafka-example/cmd"

func main() {
	// cmd.Execute()

	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-quit:
				fmt.Println("Quit")
				return
			default:
				time.Sleep(2 * time.Second)
				fmt.Println("Process")
			}
		}
	}()
	time.Sleep(500 * time.Microsecond)
	close(quit)
	time.Sleep(2 * time.Second)
}

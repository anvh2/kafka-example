package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	channel := make(chan int)
	quit := make(chan bool)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-channel:
				time.Sleep(2 * time.Second)
				fmt.Println("Process message")
			case <-quit:
				fmt.Println("Quit worker")
				return
			}
		}
	}()
	channel <- 1

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	done := make(chan bool)

	go func() {
		fmt.Println("Ctr-C to interrup")
		<-sig
		fmt.Println("Shuting down ...")
		close(quit)
		wg.Wait()
		done <- true
	}()

	<-done
	fmt.Println("Shutdown")
}

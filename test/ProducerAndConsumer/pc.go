package main

import (
	"log"
	"time"
)

func Producer(c chan int, i int) {

	log.Println("producer:", i)
	c <- i
}

func Consumer(c chan int, done chan bool) {
	for {
		select {
		case x := <-c:
			log.Print("Consumer:", x)
		case <-time.After(1 * time.Second):
			log.Print("time after")
		case y := <-done:
			log.Print("Consumer Done:", y)
			break
		}
	}
}

func main() {
	c := make(chan int)
	done := make(chan bool)
	go Consumer(c, done)
	Producer(c, 1)
	time.Sleep(1 * time.Second)
	Producer(c, 2)
	done <- true

	time.Sleep(1 * time.Second)
}

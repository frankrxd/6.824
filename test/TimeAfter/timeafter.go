package main

import (
	"log"
	"time"
)

func main()  {
	select {
	case <-time.After(10 * time.Second) {
		log.Println("time.After")
		time.Sleep(1*time.Second)
		time.Sleep(1*time.Second)
	}
	}
}

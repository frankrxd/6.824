package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int)
	go func() {
		ch <- 1
		ch <- 2
		time.Sleep(time.Second)
		close(ch)
	}()

	go func() {
		for {
			elem, ok := <-ch
			if ok == false {
				fmt.Println("ok == false")
				break
			} else {
				fmt.Println("elem get ", elem)
			}
		}
	}()

	time.Sleep(100 * time.Second)
}


package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int) //创建一个无缓存channel

	//新建一个goroutine
	go func() {
		for i := 0; i < 5; i++ {
			ch <- i //往通道写数据
			time.Sleep(time.Second)
		}
		close(ch)
		//不需要再写数据时，关闭channel

		//ch <- 666 //关闭channel后无法再发送数据

	}() //别忘了()
	for num := range ch {
		fmt.Println("num = ", num)
	}


}
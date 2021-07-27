package main

import "fmt"

func main() {
	myMap := make(map[int]int)
	myMap[0] = 100
	count,_ := myMap[0]
	count++
	fmt.Println(myMap)
}

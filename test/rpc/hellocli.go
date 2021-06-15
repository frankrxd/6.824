package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1120")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply string
	err = client.Call("HelloCall.Hello", "world", &reply)
	if err != nil {
		log.Fatal("rpc call error:", err)
	} else {
		fmt.Println(reply)
	}

}

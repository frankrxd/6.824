package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type HelloCall int
type HelloStruct struct {
	ID   int
	Term int
}
type HelloArgs struct {
	Info []HelloStruct
	Cmd  interface{}
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1120")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply string
	args := HelloArgs{Info: []HelloStruct{{1,1},{2,2}},Cmd: 10	}
	err = client.Call("HelloCall.Hello", &args, &reply)
	if err != nil {
		log.Fatal("rpc call error:", err)
	} else {
		fmt.Println(reply)
	}

}

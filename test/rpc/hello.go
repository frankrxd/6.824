package main

import (
	"fmt"
	"net/http"
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

func (t *HelloCall) Hello(args *HelloArgs, reply *string) error {
	fmt.Println(*args)
	*reply = "Hello"
	return nil
}

func main() {
	hellorpc := new(HelloCall)
	rpc.Register(hellorpc)
	rpc.HandleHTTP()
	err := http.ListenAndServe(":1120", nil)
	if err != nil {
		fmt.Println(err.Error())
	}
}

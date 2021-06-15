package main

import (
	"fmt"
	"net/http"
	"net/rpc"
)

type HelloCall int

func (t *HelloCall) Hello(args *string, reply *string) error {
	*reply = "Hello," + *args + "!"
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

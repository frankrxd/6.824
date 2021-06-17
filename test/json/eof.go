package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

func main()  {
	filename := "mr-7-0"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err = dec.Decode(&kv); err != nil {
			log.Println("json.Deode:",err)
			break
		} else {
			fmt.Println(kv)
		}
	}
	file.Close()
}

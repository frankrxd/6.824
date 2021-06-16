package utils

import (
	"log"
	"os"
)

var Info *log.Logger

func init() {
	file, err := os.OpenFile("./log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open info log file :", err)
	}
	// Info = log.New(io.MultiWriter(os.Stdout, file), "INFO:", log.Ldate|log.Ltime|log.Lshortfile)

	Info = log.New(file, "INFO:", log.Ldate|log.Ltime|log.Lshortfile)
}
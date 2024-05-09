package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
)

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Unable to connect to rabbitmq: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to rabbitmq")

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	fmt.Println("Received an interrupt, stopping services...")
}

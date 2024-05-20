package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Unable to connect to rabbitmq: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to rabbitmq")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to open channel: %v", err)
	}

	err = pubsub.SubscribeToGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerGameLogs())
	if err != nil {
		log.Fatalf("Unable to declare and bind to queue: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending a pause message...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Unable to publish message: %v", err)
			}
		case "resume":
			fmt.Println("Sending a resume message...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Unable to publish message: %v", err)
			}
		case "quit":
			fmt.Println("Existing the server...")
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func handlerGameLogs() func(gl routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("Unable to write log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

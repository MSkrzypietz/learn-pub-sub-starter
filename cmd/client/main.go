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
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Unable to connect to rabbitmq: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to rabbitmq")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to open channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeToJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient, handlerPause(gs))
	if err != nil {
		log.Fatalf("Unable to subscribe to game pause event: %v", err)
	}

	err = pubsub.SubscribeToJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gs))
	if err != nil {
		log.Fatalf("Unable to subscribe to army move event: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			err = gs.CommandSpawn(input)
			if err != nil {
				fmt.Println("Error spawning:", err)
			}
		case "move":
			move, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println("Error moving:", err)
				continue
			}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, move)
			if err != nil {
				fmt.Println("Error publishing army move event:", err)
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(state routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		if gs.HandleMove(move) == gamelogic.MoveOutcomeSamePlayer {
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}

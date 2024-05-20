package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"time"
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

	err = pubsub.SubscribeToJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gs, ch))
	if err != nil {
		log.Fatalf("Unable to subscribe to army move event: %v", err)
	}

	err = pubsub.SubscribeToJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gs, ch, username))
	if err != nil {
		log.Fatalf("Unable to subscribe to war recognitions event: %v", err)
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
			if len(input) < 2 {
				fmt.Println("Spamming requires the count as the second argument")
				continue
			}

			count, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("Spamming requires the count to be an integer")
				continue
			}

			for i := 0; i < count; i++ {
				gl := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     gamelogic.GetMaliciousLog(),
					Username:    username,
				}
				pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gl)
			}
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

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		switch gs.HandleMove(move) {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			rw := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), rw)
			if err != nil {
				fmt.Println("Error publishing war recognition event:", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}

		log.Println("Unknown move")
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel, username string) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			return publishGameLog(ch, rw, username, msg)
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			return publishGameLog(ch, rw, username, msg)
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			return publishGameLog(ch, rw, username, msg)
		}

		log.Println("Unknown war outcome:", outcome)
		return pubsub.NackDiscard
	}
}

func publishGameLog(ch *amqp.Channel, rw gamelogic.RecognitionOfWar, username, logMessage string) pubsub.AckType {
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     logMessage,
		Username:    username,
	}
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+rw.Attacker.Username, gl)
	if err != nil {
		log.Println("Error publishing log:", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}

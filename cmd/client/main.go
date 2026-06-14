package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672"
	amqpConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer amqpConnection.Close()

	ch, err := amqpConnection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(gameState),
	); err != nil {
		log.Fatal(err)
	}

	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		routing.ArmyMovesPrefix+".*",
		pubsub.QueueTypeTransient,
		handlerMove(gameState, ch),
	); err != nil {
		log.Fatal(err)
	}

	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.QueueTypeDurable,
		handlerWar(gameState, ch),
	); err != nil {
		log.Fatal(err)
	}

loop:
	for {
		input := gamelogic.GetInput()
		if len(input) < 1 {
			log.Println("Please enter a command")
			continue
		}
		switch input[0] {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				log.Println(err)
				continue
			}
		case "move":
			armyMove, err := gameState.CommandMove(input)
			if err != nil {
				log.Println(err)
				continue
			}

			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				armyMove,
			); err != nil {
				log.Println(err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			break loop
		case "spam":
			if len(input) != 2 {
				log.Println("usage 'spam N' where N is the number of messages")
				continue
			}
			n, err := strconv.Atoi(input[1])
			if err != nil {
				log.Println(err)
				continue
			}
			for range n {
				maliciousLog := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(
					ch,
					"peril_topic",
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     maliciousLog,
						Username:    username,
					},
				)
				if err != nil {
					log.Println("failed to generate maliciousLog")
				}
			}

		default:
			log.Println("Please enter a valid command.")
		}
	}
}

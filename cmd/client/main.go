package main

import (
	"fmt"
	"log"

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = pubsub.DeclareAndBind(
		amqpConnection,
		"peril_direct",
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)

loop:
	for {
		input := gamelogic.GetInput()
		if len(input) < 1 {
			log.Println("Please enter a command")
		}
		switch input[0] {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				log.Println(err)
				continue
			}
		case "move":
			_, err := gameState.CommandMove(input)
			if err != nil {
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
			log.Println("Spamming not allowed yet!")
		default:
			log.Println("Please enter a valid command.")
		}
	}
}

// 	fmt.Println("Client connection Succesful!")
// 	// wait for ctrl+c
// 	signalChan := make(chan os.Signal, 1)
// 	signal.Notify(signalChan, os.Interrupt)
// 	<-signalChan
// 	fmt.Println("\nSIGIN received, shutting down...")
// }

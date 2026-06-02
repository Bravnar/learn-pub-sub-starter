package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func runCommand(msg, command string, channel *amqp.Channel) {
	isPaused := command == "resume"
	log.Println(msg)
	if err := pubsub.PublishJSON(
		channel,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: isPaused,
		},
	); err != nil {
		log.Fatal("error occured in the publishJSON function:", err)
	}
}

func main() {
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

	connectionString := "amqp://guest:guest@localhost:5672"
	amqpConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer amqpConnection.Close()

	ch, err := amqpConnection.Channel()
	if err != nil {
		log.Fatal("failed to create channel:", err)
	}

	fmt.Println("Amqp Connection Successful!")
loop:
	for {
		input := gamelogic.GetInput()
		if len(input) < 1 {
			log.Println("Please enter a command")
		}
		switch input[0] {
		case "pause":
			runCommand("running pause command...", "pause", ch)
		case "resume":
			runCommand("running resume command...", "command", ch)
		case "quit":
			log.Println("Exiting...")
			break loop
		default:
			log.Println("Please enter a valid command.")
		}
	}
}

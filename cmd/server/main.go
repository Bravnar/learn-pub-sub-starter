package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672"
	amqpConnection, err := amqp.Dial(connectionString)
	if err != nil {
		os.Exit(1)
	}
	defer amqpConnection.Close()

	fmt.Println("Amqp Connection Successful!")
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nSIGINT received, shutting down...")
}

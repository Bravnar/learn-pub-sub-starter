package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var gobData bytes.Buffer
	enc := gob.NewEncoder(&gobData)

	// Encode
	if err := enc.Encode(val); err != nil {
		return err
	}

	// Publish
	if err := ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        gobData.Bytes(),
		},
	); err != nil {
		return err
	}
	return nil
}

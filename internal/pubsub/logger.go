package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func EncodeGob[T any](val T) ([]byte, error) {
	var gobData bytes.Buffer
	enc := gob.NewEncoder(&gobData)

	if err := enc.Encode(val); err != nil {
		return gobData.Bytes(), err
	}
	return gobData.Bytes(), nil
}

func DecodeGob[T any](data []byte) (T, error) {
	var target T
	dec := gob.NewDecoder(bytes.NewBuffer(data))

	if err := dec.Decode(&target); err != nil {
		return target, err
	}
	return target, nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	err := subscribe(conn, exchange, queueName, key, queueType, handler, DecodeGob)
	if err != nil {
		return err
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	gobBytes, err := EncodeGob(val)
	if err != nil {
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
			Body:        gobBytes,
		},
	); err != nil {
		return err
	}
	return nil
}

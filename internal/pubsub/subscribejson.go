package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	go func() {
		for chann := range deliveryChan {
			var target T
			if err := json.Unmarshal(chann.Body, &target); err != nil {
				continue
			}
			ackType := handler(target)
			switch ackType {
			case Ack:
				err = chann.Ack(false)
				if err != nil {
					continue
				}
			case NackRequeue:
				err = chann.Nack(false, true)
				if err != nil {
					continue
				}
			case NackDiscard:
				err = chann.Nack(false, false)
				if err != nil {
					continue
				}
			}
		}
	}()
	return nil
}

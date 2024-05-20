package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data},
	)
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes()},
	)
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := simpleQueueType == Durable
	autoDelete := simpleQueueType == Transient
	exclusive := simpleQueueType == Transient
	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func SubscribeToJSON[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType, handler func(T) AckType) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			var msg T
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				log.Println(err)
				continue
			}

			switch handler(msg) {
			case Ack:
				log.Println("Responding with ack")
				err = d.Ack(false)
			case NackRequeue:
				log.Println("Responding with nack requeue")
				err = d.Nack(false, true)
			case NackDiscard:
				log.Println("Responding with nack discard")
				err = d.Nack(false, false)
			}
			if err != nil {
				log.Println(err)
			}
		}
	}()

	return nil
}

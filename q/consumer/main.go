package main

import (
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	//conn, err := amqp.Dial("amqp://guest:guest@work2:5673/")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangName := "ex_01"
	queueName := "q_01"
	consumer := "c_01"

	err = ch.ExchangeDeclare(
		exchangName, // name
		"fanout",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		exchangName, // exchange
		false,
		nil,
	)

	msgs, err := ch.Consume(
		q.Name,   // queue
		consumer, // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to register a consumer")

main:
	for {
		select {
		case d := <-msgs:
			err := d.Ack(false)
			if err != nil {
				log.Printf("Ack Error: %s", err.Error())
			}
			log.Printf("Received a message: %s", d.Body)
			os.Stderr.Sync()
		case <-time.After(time.Second * 10):
			break main
		}
	}
}

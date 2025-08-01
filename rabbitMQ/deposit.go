package rabbitmqconnect

import (
	"context"
	"log"
	"time"

	errors "github.com/celalsahinaltinisik/exceptions"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitDepositMQ struct {
	Body      string
	QueueName string
}

func (r *RabbitDepositMQ) Deposit() {

	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	// log.Println(ch)
	q, err := ch.QueueDeclare(
		r.QueueName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	errors.FailOnError(err, "Failed to declare a deposit queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := r.Body
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	errors.FailOnError(err, "Failed data deposit")
	log.Printf(" [x] Sent %s\n", body)
}

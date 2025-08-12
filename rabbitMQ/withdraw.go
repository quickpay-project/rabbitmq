package rabbitmqconnect

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitWithdrawMQ struct {
	Body      string
	QueueName string
	Headers   map[string]string
}

// genCorrelationID สร้าง UUID/Random string
func genCorrelationID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (r *RabbitWithdrawMQ) WithdrawRPC() (map[string]interface{}, error) {
	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	replyQueue, err := ch.QueueDeclare(
		"", false, true, true, false, nil,
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(replyQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	corrID := genCorrelationID()

	headers := amqp.Table{}
	for k, v := range r.Headers {
		headers[k] = v
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"", r.QueueName, false, false,
		amqp.Publishing{
			ContentType:   "application/json",
			Body:          []byte(r.Body),
			Headers:       headers,
			CorrelationId: corrID,
			ReplyTo:       replyQueue.Name,
		})
	if err != nil {
		return nil, err
	}

	timeout := time.After(90 * time.Second)
	for {
		select {
		case msg := <-msgs:
			if msg.CorrelationId == corrID {
				var result map[string]interface{}
				err := json.Unmarshal(msg.Body, &result)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
		case <-timeout:
			return nil, errors.New("RPC timeout")
		}
	}
}

/*
package rabbitmqconnect

import (
	"context"
	"time"

	errors "github.com/celalsahinaltinisik/exceptions"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitWithdrawMQ struct {
	Body      string
	QueueName string
	Headers   map[string]string // เพิ่มตรงนี้เพื่อส่ง Header
}

func (r *RabbitWithdrawMQ) Withdraw() {

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

	errors.FailOnError(err, "Failed to declare a withdraw queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// แปลง headers map[string]string → amqp.Table
	headers := amqp.Table{}
	for key, value := range r.Headers {
		headers[key] = value
	}

	body := r.Body
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:     headers, // ใส่ Header ตรงนี้
		})
	errors.FailOnError(err, "Failed data withdraw")
	// log.Printf(" [x] Sent %s\n", body)
	// log.Printf(" [x] Sent Headers %s\n", headers)
}
*/

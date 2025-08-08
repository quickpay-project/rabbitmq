package rabbitmqconnect

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

func (r *RabbitWithdrawMQ) WithdrawRPC() ([]byte, error) {
	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	// ✅ Step 1: ประกาศ reply queue ชั่วคราว
	replyQueue, err := ch.QueueDeclare(
		"",    // random name (exclusive)
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, err
	}

	// ✅ Step 2: สร้าง consumer ที่รอฟัง reply
	msgs, err := ch.Consume(
		replyQueue.Name,
		"",
		true,  // auto-ack
		false, // exclusive
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// ✅ Step 3: สร้าง CorrelationId
	corrID := genCorrelationID()

	// ✅ Step 4: แปลง headers map[string]string → amqp.Table
	headers := amqp.Table{}
	for key, value := range r.Headers {
		headers[key] = value
	}

	// ✅ Step 5: Publish message พร้อม ReplyTo และ CorrelationId
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",          // default exchange
		r.QueueName, // routing key
		false,
		false,
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

	// ✅ Step 6: รอฟัง response เฉพาะ corrID ที่ส่งไป
	timeout := time.After(30 * time.Second) // ปรับตามความเหมาะสม
	for {
		select {
		case msg := <-msgs:
			if msg.CorrelationId == corrID {
				return msg.Body, nil
			}
		case <-timeout:
			return nil, errors.New("RPC timeout waiting for response")
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

package rabbitmqconnect

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitDepositMQ struct {
	Body      string
	QueueName string
	Headers   map[string]string
}

func (r *RabbitDepositMQ) DepositRPC() ([]byte, error) {
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
	timeout := time.After(90 * time.Second) // ปรับตามความเหมาะสม
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

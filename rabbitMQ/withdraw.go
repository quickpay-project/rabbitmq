package rabbitmqconnect

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitWithdrawMQ struct {
	Body      string
	QueueName string
	Headers   map[string]string
}

func (r *RabbitWithdrawMQ) ConswithdrawRPC() {
	if err := InitDB(); err != nil {
		log.Fatalf("❌ InitDB failed: %v", err)
	}
	defer db.Close()

	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	q, err := ch.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Queue declare error: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Consume error: %v", err)
	}

	for d := range msgs {
		headers := map[string]interface{}{}
		for k, v := range d.Headers {
			headers[k] = v
		}

		// ทำงานตามปกติ
		httpStatus, respBody, txnID, sendErr := sendToExternalWithdrawAPI(d.Body, headers)

		status := "sent"
		errMsg := ""
		if sendErr != nil || httpStatus >= 500 {
			status = "failed"
			if sendErr != nil {
				errMsg = sendErr.Error()
			}
		}

		_, _ = insertWithdrawLog(q.Name, d.Body, headers, httpStatus, respBody, status, 1, errMsg, txnID)

		// ส่ง response กลับไปหา client
		response := map[string]interface{}{
			"http_status": httpStatus,
			"body":        respBody,
			"status":      status,
			"error":       errMsg,
			"txn_id":      txnID,
		}
		respJSON, _ := json.Marshal(response)

		err = ch.Publish(
			"",        // default exchange
			d.ReplyTo, // reply queue จาก client
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId, // ต้องใช้ค่าจาก client
				Body:          respJSON,
			},
		)
		if err != nil {
			log.Printf("❌ Failed to send RPC response: %v", err)
		}

		_ = d.Ack(false)
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

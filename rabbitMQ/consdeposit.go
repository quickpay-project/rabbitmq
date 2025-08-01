package rabbitmqconnect

import (
	"bytes"
	"log"
	"net/http"

	errors "github.com/celalsahinaltinisik/exceptions"
)

func sendToExternalDepositAPI(data []byte) error {
	// กำหนด URL ปลายทาง
	apiURL := "https://webhook.site/3fef26b2-eb5e-4ae4-95a5-e01dd9890336"

	// POST ไปยัง API
	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Println("❌ Failed to send to external API:", err)
		return err
	}
	defer resp.Body.Close()

	log.Println("✅ Data sent to API:", resp.Status)
	return nil
}

func (r *RabbitMQ) Consdeposit() {
	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	q, err := ch.QueueDeclare(
		r.QueueName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	errors.FailOnError(err, "Failed to declare a deposit queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // manual ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	errors.FailOnError(err, "Failed to register a deposit consumer")

	k := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("📩 Deposit รับ: %s", d.Body)

			// ส่งออก API
			err := sendToExternalDepositAPI(d.Body)
			if err != nil {
				log.Println("❌  Deposit ส่งไม่สำเร็จ:", err)
				_ = d.Nack(false, true) // แจ้ง RabbitMQ ว่าข้อความนี้ยังส่งไม่สำเร็จ
			} else {
				log.Println("✅ Deposit ส่งสำเร็จ")
				_ = d.Ack(false) // สำเร็จ
			}
		}

	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-k
}

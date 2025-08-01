package rabbitmqconnect

import (
	"bytes"
	"log"
	"net/http"
	"os"

	errors "github.com/celalsahinaltinisik/exceptions"
)

func sendToExternalWithdrawAPI(data []byte, headers map[string]interface{}) error {
	apiURL := os.Getenv("WITHDRAW_URL")

	// สร้าง HTTP request
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	if err != nil {
		log.Println("❌ Failed to create HTTP request:", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	// เพิ่ม custom headers จาก RabbitMQ
	for key, value := range headers {
		if strVal, ok := value.(string); ok {
			req.Header.Set(key, strVal)
		}
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("❌ Failed to send to external API:", err)
		return err
	}
	defer resp.Body.Close()

	log.Println("✅ Data sent to API:", resp.Status)
	return nil
}

func (r *RabbitWithdrawMQ) Conswithdraw() {
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
	errors.FailOnError(err, "Failed to declare a withdraw queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // manual ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	errors.FailOnError(err, "Failed to register a withdraw consumer")

	k := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("📩 Withdraw รับ: %s", d.Body)

			// ดึง headers จาก RabbitMQ
			headers := map[string]interface{}{}
			for key, val := range d.Headers {
				headers[key] = val
			}

			// ส่งออก API
			err := sendToExternalWithdrawAPI(d.Body, headers)
			if err != nil {
				log.Println("❌  Withdraw ส่งไม่สำเร็จ:", err)
				_ = d.Nack(false, true) // แจ้ง RabbitMQ ว่าข้อความนี้ยังส่งไม่สำเร็จ
			} else {
				log.Println("✅ Withdraw ส่งสำเร็จ")
				_ = d.Ack(false) // สำเร็จ
			}
		}

	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-k
}

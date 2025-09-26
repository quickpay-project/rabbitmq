package rabbitmqconnect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ===== INSERT LOG =====
func insertTransactionLog(
	queueName string,
	body []byte,
	headers map[string]interface{},
	httpStatus int,
	httpRespBody string,
	statusStr string,
	apiURL string,
	errMsg string,
) (int64, error) {

	if db == nil {
		return 0, errors.New("db not initialized")
	}

	headersJSON, _ := json.Marshal(headers)
	var msgJSON json.RawMessage
	if json.Valid(body) {
		msgJSON = body
	} else {
		wrapped, _ := json.Marshal(map[string]string{"raw": string(body)})
		msgJSON = wrapped
	}

	query := `
INSERT INTO transaction_logs 
(queue_name, message_body, headers, http_status, http_response_body, status, api_url, error_message, created_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, now(), now())
RETURNING id;`

	var id int64
	err := db.QueryRow(query, queueName, msgJSON, headersJSON, httpStatus, httpRespBody, statusStr, apiURL, errMsg).Scan(&id)
	return id, err
}

// ===== ยิงไป API TRANSACTION_URL_GROUP1 =====
func sendToAllTransactionAPIs(queueName string, originalBody []byte, headers map[string]interface{}, data []byte) {
	apiURL := os.Getenv("TRANSACTION_URL_GROUP1")
	if apiURL == "" {
		log.Println("⚠️ TRANSACTION_URL_GROUP1 is empty, skip sending")
		return
	}

	client := &http.Client{Timeout: 30 * time.Second}

	req, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ Error sending to %s: %v", apiURL, err)
		_, _ = insertTransactionLog(queueName, originalBody, headers, 0, "", "failed", apiURL, err.Error())
		return
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	// ✅ Log ทุกกรณี success/fail
	status := "sent"
	errMsg := ""
	if resp.StatusCode >= 400 {
		status = "failed"
		errMsg = string(respBytes)
		log.Printf("❌ API Error %s | Status: %d | Resp: %s", apiURL, resp.StatusCode, errMsg)
	} else {
		log.Printf("✅ Sent to %s | Status: %d | Resp: %s", apiURL, resp.StatusCode, string(respBytes))
	}

	_, _ = insertTransactionLog(queueName, originalBody, headers, resp.StatusCode, string(respBytes), status, apiURL, errMsg)
}

// ===== RPC Consumer =====
func processTransactionMessage(d amqp.Delivery, ch *amqp.Channel, queueName string) {
	headers := map[string]interface{}{}
	for k, v := range d.Headers {
		headers[k] = v
	}

	// ✅ Print payload และ headers จากต้นทาง
	//log.Printf("📥 Incoming Message from Queue [%s]: %s", queueName, string(d.Body))
	//log.Printf("📥 Headers: %+v", headers)

	// ✅ forward payload ตรง ๆ และ log ทุก response
	sendToAllTransactionAPIs(queueName, d.Body, headers, d.Body)

	// ✅ reply กลับไปยังต้นทาง (optional)
	if d.ReplyTo != "" {
		resp, _ := json.Marshal(map[string]interface{}{
			"status":  200,
			"message": "transaction update requests sent (check DB logs for details)",
			"echo":    json.RawMessage(d.Body), // ✅ ส่ง payload กลับให้ดูด้วย
		})
		_ = ch.PublishWithContext(context.Background(),
			"",
			d.ReplyTo,
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          resp,
			})
	}

	d.Ack(false)
}

// ===== RPC CONSUMER =====
func (r *RabbitTransactionMQ) ConstransactionRPC() {
	if err := InitDB(); err != nil {
		log.Fatalf("❌ InitDB failed: %v", err)
	}
	defer db.Close()

	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	q, err := ch.QueueDeclare(r.QueueName, false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Queue declare error: %v", err)
	}

	workerCountStr := os.Getenv("TRANSACTION_LIMIT")
	workerCount, err := strconv.Atoi(workerCountStr)
	if err != nil {
		log.Fatalf("❌ invalid TRANSACTION_LIMIT: %v", err)
	}

	if err := ch.Qos(workerCount, 0, false); err != nil {
		log.Fatalf("❌ QoS set error: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Consume error: %v", err)
	}

	log.Printf("[*] Waiting for RPC requests on queue: %s", q.Name)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for d := range msgs {
				processTransactionMessage(d, ch, r.QueueName)
			}
		}(i)
	}

	wg.Wait()
}

package rabbitmqconnect

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var db *sql.DB

// ========== DB INIT ==========
func InitDB() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// ========== INSERT LOG ==========
func insertWithdrawLog(queueName string, body []byte, headers map[string]interface{},
	httpStatus int, httpRespBody string, statusStr string, attempts int, errMsg string, txnID string) (int64, error) {

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
INSERT INTO withdraw_logs 
(queue_name, message_body, headers, http_status, http_response_body, status, attempts, error_message, api_transaction_id, created_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, $9, now(), now())
RETURNING id;`

	var id int64
	err := db.QueryRow(query, queueName, msgJSON, headersJSON, httpStatus, httpRespBody, statusStr, attempts, errMsg, txnID).Scan(&id)
	return id, err
}

// ========== SEND TO EXTERNAL API ==========
func sendToExternalWithdrawAPI(data []byte, headers map[string]interface{}) (int, string, string, error) {
	apiURL := os.Getenv("WITHDRAW_URL")
	if apiURL == "" {
		return 0, "", "", errors.New("WITHDRAW_URL not set")
	}

	// สร้าง request
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	if err != nil {
		return 0, "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		switch val := v.(type) {
		case string:
			req.Header.Set(k, val)
		case []byte:
			req.Header.Set(k, string(val))
		default:
			b, _ := json.Marshal(val)
			req.Header.Set(k, string(b))
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()

	// อ่าน body เป็น []byte
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", "", err
	}

	// แปลงเป็น string เพื่อ log safely
	bodyStr := string(respBytes)
	log.Printf("📥 Response body: %s", bodyStr)

	// แยก transaction_id จาก details[0]
	txnID := ""
	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err == nil {
		if dataMap, ok := parsed["data"].(map[string]interface{}); ok {
			if details, ok := dataMap["details"].([]interface{}); ok && len(details) > 0 {
				if first, ok := details[0].(map[string]interface{}); ok {
					if val, ok := first["transaction_id"].(string); ok {
						txnID = val
					}
				}
			}
		}
	}

	return resp.StatusCode, bodyStr, txnID, nil
}

// ========== RPC CONSUMER ==========
func (r *RabbitWithdrawMQ) ConswithdrawRPC() {
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

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Consume error: %v", err)
	}

	log.Printf("[*] Waiting for RPC requests on queue: %s", q.Name)
	for d := range msgs {
		headers := map[string]interface{}{}
		for k, v := range d.Headers {
			headers[k] = v
		}

		httpStatus, respBody, txnID, sendErr := sendToExternalWithdrawAPI(d.Body, headers)

		log.Printf("HTTP Status: %d", httpStatus)
		log.Printf("Transaction ID: %s", txnID)
		log.Printf("Body length: %d", len(respBody))

		status := "sent"
		errMsg := ""
		if sendErr != nil || httpStatus >= 500 {
			status = "failed"
			if sendErr != nil {
				errMsg = sendErr.Error()
			}
		}
		_, _ = insertWithdrawLog(r.QueueName, d.Body, headers, httpStatus, respBody, status, 1, errMsg, txnID)

		if d.ReplyTo != "" {
			var jsonBody []byte
			if json.Valid([]byte(respBody)) {
				jsonBody = []byte(respBody)
			} else {
				jsonBody, _ = json.Marshal(map[string]interface{}{
					"status": httpStatus,
					"body":   respBody,
					"error":  errMsg,
				})
			}

			_ = ch.PublishWithContext(context.Background(),
				"",
				d.ReplyTo,
				false,
				false,
				amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          jsonBody,
				})
		}

		d.Ack(false)
	}
}

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

// InitDB initializes the database connection
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

// insertWithdrawLog logs the message and response to the database
func insertWithdrawLog(queueName string, body []byte, headers map[string]interface{}, httpStatus int, httpRespBody string, statusStr string, attempts int, errMsg string, txnID string) (int64, error) {
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

// sendToExternalWithdrawAPI sends the payload to external API and returns status, response body, and error
func sendToExternalWithdrawAPI(data []byte, headers map[string]interface{}) (int, string, string, error) {
	apiURL := os.Getenv("WITHDRAW_URL")
	if apiURL == "" {
		return 0, "", "", errors.New("WITHDRAW_URL not set")
	}

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

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", "", err
	}

	bodyStr := string(respBytes)

	var parsed map[string]interface{}
	_ = json.Unmarshal(respBytes, &parsed)
	txnID := ""
	if val, ok := parsed["transaction_id"].(string); ok {
		txnID = val
	}

	return resp.StatusCode, bodyStr, txnID, nil
}

// Conswithdraw handles message consumption, forwarding, and logging
/*
func (r *RabbitWithdrawMQ) Conswithdraw() {
	// 🟢 Connect DB
	if err := InitDB(); err != nil {
		log.Fatalf("❌ InitDB failed: %v", err)
	}
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				log.Printf("⚠️ Failed to close DB: %v", err)
			} else {
				log.Println("✅ Database connection closed.")
			}
		}
	}()

	// 🟢 Connect RabbitMQ
	conn, ch := ConnectMQ()
	if conn == nil || ch == nil {
		log.Println("❌ Failed to connect to RabbitMQ")
		return
	}
	defer CloseMQ(conn, ch)

	q, err := ch.QueueDeclare(r.QueueName, false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Queue declare error: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Consume error: %v", err)
	}

	log.Printf("[*] Waiting for messages from queue: %s", q.Name)
	for d := range msgs {
		headers := map[string]interface{}{}
		for k, v := range d.Headers {
			headers[k] = v
		}

		attempt := 1
		if val, ok := headers["x-attempts"].(int32); ok {
			attempt = int(val)
		}

		if !json.Valid(d.Body) {
			log.Println("❌ Invalid JSON, rejecting")
			_ = d.Nack(false, false)
			continue
		}

		httpStatus, respBody, txnID, sendErr := sendToExternalWithdrawAPI(d.Body, headers)

		if sendErr != nil || httpStatus >= 500 {
			_, _ = insertWithdrawLog(q.Name, d.Body, headers, httpStatus, respBody, "failed", attempt, sendErr.Error(), txnID)
			_ = d.Nack(false, true)
		} else {
			_, _ = insertWithdrawLog(q.Name, d.Body, headers, httpStatus, respBody, "sent", attempt, "", txnID)
			_ = d.Ack(false)
		}
	}
}
*/

func (r *RabbitWithdrawMQ) Conswithdraw() {
	// Init DB
	if err := InitDB(); err != nil {
		log.Fatalf("❌ InitDB failed: %v", err)
	}
	defer db.Close()

	conn, ch := ConnectMQ()
	defer CloseMQ(conn, ch)

	// Queue RPC
	q, err := ch.QueueDeclare(
		r.QueueName, // เช่น: "withdraw_rpc_queue"
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("❌ Failed to declare queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, "", false, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("❌ Failed to consume: %v", err)
	}

	log.Printf("📡 [*] Awaiting RPC requests on queue: %s", q.Name)
	for d := range msgs {
		headers := make(map[string]interface{})
		for k, v := range d.Headers {
			headers[k] = v
		}

		statusCode, respBody, txnID, err := sendToExternalWithdrawAPI(d.Body, headers)
		response := map[string]interface{}{
			"status_code":    statusCode,
			"body":           respBody,
			"transaction_id": txnID,
		}

		var errMsg string
		resultStatus := "sent"
		if err != nil || statusCode >= 500 {
			errMsg = err.Error()
			resultStatus = "failed"
		}

		_, _ = insertWithdrawLog(r.QueueName, d.Body, headers, statusCode, respBody, resultStatus, 1, errMsg, txnID)

		// ส่ง response กลับไป queue ที่ระบุใน ReplyTo
		resBody, _ := json.Marshal(response)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = ch.PublishWithContext(
			ctx,
			"",        // default exchange
			d.ReplyTo, // reply queue
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          resBody,
			},
		)
		if err != nil {
			log.Printf("❌ Failed to send RPC reply: %v", err)
		} else {
			log.Printf("✅ RPC reply sent, txnID: %s", txnID)
		}

		_ = d.Ack(false)
	}
}

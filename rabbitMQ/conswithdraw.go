package rabbitmqconnect

import (
	"bytes"
	"compress/gzip"
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
func insertWithdrawLog(queueName string, body []byte, headers map[string]interface{},
	httpStatus int, httpRespBody string, statusStr string, attempts int, errMsg string, txnID string) (int64, error) {

	if db == nil {
		return 0, errors.New("db not initialized")
	}

	// ✅ ทำให้ body สวยงามก่อนเก็บ
	var msgJSON json.RawMessage
	if json.Valid(body) {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err == nil {
			msgJSON = pretty.Bytes()
		} else {
			msgJSON = body
		}
	} else {
		wrapped, _ := json.Marshal(map[string]string{"raw": string(body)})
		msgJSON = wrapped
	}

	// ✅ ทำให้ httpRespBody เป็น JSON สวยงามถ้าทำได้
	var respPretty string
	var parsed map[string]interface{}
	if json.Unmarshal([]byte(httpRespBody), &parsed) == nil {
		if prettyBytes, err := json.MarshalIndent(parsed, "", "  "); err == nil {
			respPretty = string(prettyBytes)
		} else {
			respPretty = httpRespBody
		}
	} else {
		respPretty = httpRespBody
	}

	headersJSON, _ := json.Marshal(headers)

	query := `
INSERT INTO withdraw_logs 
(queue_name, message_body, headers, http_status, http_response_body, status, attempts, error_message, api_transaction_id, created_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, $9, now(), now())
RETURNING id;`

	var id int64
	err := db.QueryRow(query, queueName, msgJSON, headersJSON, httpStatus, respPretty, statusStr, attempts, errMsg, txnID).Scan(&id)
	return id, err
}

// sendToExternalWithdrawAPI sends the payload to external API and returns status, response body, and error
func sendToExternalWithdrawAPI(data []byte, headers map[string]interface{}) (int, string, string, error) {
	apiURL := os.Getenv("WITHDRAW_URL")
	log.Println("WITHDRAW_URL:", apiURL)

	if apiURL == "" {
		return 0, "", "", errors.New("WITHDRAW_URL not set")
	}

	type APIResponse struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Order struct {
				OperatorOrderID string  `json:"operator_order_id"`
				Amount          float64 `json:"amount"`
			} `json:"order"`
			Details []struct {
				TransactionID string  `json:"transaction_id"`
				Amount        float64 `json:"amount"`
			} `json:"details"`
		} `json:"data"`
	}

	var statusCode int
	var txnID string
	var jsonStr string
	var amount float64

	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
		if err != nil {
			return 0, "", "", err
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip, deflate")
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
			if attempt == 3 {
				return 0, "", "", err
			}
			time.Sleep(2 * time.Second)
			continue
		}
		defer resp.Body.Close()

		var reader io.ReadCloser
		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			reader, err = gzip.NewReader(resp.Body)
			if err != nil {
				return resp.StatusCode, "", "", err
			}
			defer reader.Close()
		default:
			reader = resp.Body
		}

		respBytes, err := io.ReadAll(reader)
		if err != nil {
			if attempt == 3 {
				return resp.StatusCode, "", "", err
			}
			time.Sleep(2 * time.Second)
			continue
		}

		statusCode = resp.StatusCode
		jsonStr = string(respBytes)

		// ✅ Parse JSON
		var parsed APIResponse
		if err := json.Unmarshal(respBytes, &parsed); err == nil {
			if len(parsed.Data.Details) > 0 {
				txnID = parsed.Data.Details[0].TransactionID
				amount = parsed.Data.Details[0].Amount
			}
		}

		// ✅ บันทึกลง DB
		if db != nil {
			_, err := insertWithdrawLog(
				"withdraw",
				data,
				headers,
				statusCode,
				jsonStr,
				"sent",
				1,
				"",
				txnID,
			)
			if err != nil {
				log.Printf("⚠️ Failed to insert withdraw log: %v", err)
			} else {
				log.Printf("✅ Withdraw log inserted: txnID=%s, amount=%.2f", txnID, amount)
			}
		}

		if statusCode == http.StatusOK {
			break
		} else {
			time.Sleep(2 * time.Second)
		}
	}

	return statusCode, jsonStr, txnID, nil
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

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
)

var dbd *sql.DB

// Call InitDB early in your main() or package setup
func InitDBD() error {

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}

	var err error
	dbd, err = sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	// adjust for your environment
	dbd.SetMaxOpenConns(20)
	dbd.SetMaxIdleConns(5)
	dbd.SetConnMaxLifetime(time.Minute * 10)

	// Ping to verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return dbd.PingContext(ctx)
}

// insertDepositLog inserts a log row and returns the inserted id (or error)
func insertDepositLog(queueName string, body []byte, headers map[string]interface{}, httpStatus int, httpRespBody string, statusStr string, attempts int, errMsg string) (int64, error) {
	if db == nil {
		return 0, errors.New("db not initialized")
	}

	headersJSON, _ := json.Marshal(headers)
	// attempt to store body as JSONB; if not JSON, store as text inside json
	var msgJSON json.RawMessage
	if json.Valid(body) {
		msgJSON = body
	} else {
		// wrap as json: {"raw": "..."}
		wrapped, _ := json.Marshal(map[string]string{"raw": string(body)})
		msgJSON = wrapped
	}

	query := `
INSERT INTO deposit_logs 
(queue_name, message_body, headers, http_status, http_response_body, status, attempts, error_message, created_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, now(), now())
RETURNING id;
`

	var id int64
	err := db.QueryRow(query, queueName, msgJSON, headersJSON, httpStatus, httpRespBody, statusStr, attempts, errMsg).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// sendToExternalDepositAPI sends the payload to external API and returns (statusCode, responseBody, error)
func sendToExternalDepositAPI(data []byte, headers map[string]interface{}) (int, string, error) {
	apiURL := os.Getenv("DEPOSIT_URL")
	if apiURL == "" {
		return 0, "", errors.New("DEPOSIT_URL not set")
	}

	// create HTTP request
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	if err != nil {
		log.Println("❌ Failed to create HTTP request:", err)
		return 0, "", err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add custom headers (convert possible non-string values to JSON string)
	for key, value := range headers {
		switch v := value.(type) {
		case string:
			req.Header.Set(key, v)
		case []byte:
			req.Header.Set(key, string(v))
		default:
			// marshal other types to JSON
			b, _ := json.Marshal(v)
			req.Header.Set(key, string(b))
		}
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("❌ Failed to send to external API:", err)
		return 0, "", err
	}
	defer resp.Body.Close()

	// read response body
	respBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		log.Println("⚠️ failed to read response body:", readErr)
		// still return status code and partial info
		return resp.StatusCode, "", readErr
	}

	bodyStr := string(respBytes)
	log.Println("✅ Data sent to API:", resp.Status, " response length:", len(respBytes))

	return resp.StatusCode, bodyStr, nil
}

// Consdeposit consumes messages, forwards to external API and logs to DB
func (r *RabbitDepositMQ) Consdeposit() {
	conn, ch := ConnectMQ()

	// check db connect
	if err := InitDBD(); err != nil {
		log.Fatalf("❌ Failed to init DB: %v", err)
	}

	// check labbitMQ connect
	if conn == nil || ch == nil {
		log.Println("Failed to get RabbitMQ connection/channel")
		return
	}
	defer CloseMQ(conn, ch)

	q, err := ch.QueueDeclare(
		r.QueueName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Println("Failed to declare deposit queue:", err)
		return
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
		log.Println("Failed to register deposit consumer:", err)
		return
	}

	k := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("📩 Deposit received: %s", d.Body)

			// collect headers
			headers := map[string]interface{}{}
			if d.Headers != nil {
				for key, val := range d.Headers {
					headers[key] = val
				}
			}

			// Try sending to external API
			httpStatus, respBody, sendErr := sendToExternalDepositAPI(d.Body, headers)

			if sendErr != nil {
				// Log failure with attempts = 1 (or read from headers if you track attempts)
				_, insertErr := insertDepositLog(q.Name, d.Body, headers, httpStatus, respBody, "failed", 1, sendErr.Error())
				if insertErr != nil {
					log.Println("❌ Failed to insert deposit log:", insertErr)
				}
				log.Println("❌ Deposit forward failed:", sendErr)
				// Nack with requeue true (so message can be retried)
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Println("⚠️ Failed to Nack message:", nackErr)
				}
			} else {
				// success, insert a sent log
				_, insertErr := insertDepositLog(q.Name, d.Body, headers, httpStatus, respBody, "sent", 1, "")
				if insertErr != nil {
					log.Println("❌ Failed to insert deposit log:", insertErr)
				}
				log.Println("✅ Deposit forwarded successfully, acking message")
				if ackErr := d.Ack(false); ackErr != nil {
					log.Println("⚠️ Failed to Ack message:", ackErr)
				}
			}
		}
	}()

	log.Printf(" [*] Waiting for deposit messages. To exit press CTRL+C")
	<-k
}

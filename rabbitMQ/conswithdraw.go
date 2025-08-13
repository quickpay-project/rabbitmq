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
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type WithdrawRequest struct {
	Amount          float64 `json:"amount"`
	MID             string  `json:"mid"`
	CustomerOrderID string  `json:"customer_order_id"`
	CallbackURL     string  `json:"callback_url"`
}

type WithdrawResponse struct {
	Order struct {
		Cost            float64 `json:"Cost"`
		Amount          float64 `json:"amount"`
		BankCode        string  `json:"bank_code"`
		BankName        string  `json:"bank_name"`
		TotalOrder      int     `json:"total_order"`
		AccountName     string  `json:"account_name"`
		WithdrawType    string  `json:"withdraw_type"`
		AccountNumber   string  `json:"account_number"`
		WithdrawDetails any     `json:"withdraw_details"`
		CustomerOrderID string  `json:"customer_order_id"`
		OperatorOrderID string  `json:"operator_order_id"`
	} `json:"order"`
	Details []struct {
		Amount        float64 `json:"amount"`
		CreatedAt     string  `json:"created_at"`
		WithdrawID    string  `json:"withdraw_id"`
		TransactionID string  `json:"transaction_id"`
	} `json:"details"`
}

// ===== INSERT LOG =====
func insertWithdrawLog(
	queueName string,
	body []byte,
	headers map[string]interface{},
	httpStatus int,
	httpRespBody string,
	statusStr string,
	attempts int,
	errMsg string,
	txnID string,
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
INSERT INTO withdraw_logs 
(queue_name, message_body, headers, http_status, http_response_body, status, attempts, error_message, api_transaction_id, created_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, $9, now(), now())
RETURNING id;`

	var id int64
	err := db.QueryRow(query, queueName, msgJSON, headersJSON, httpStatus, httpRespBody, statusStr, attempts, errMsg, txnID).Scan(&id)
	return id, err
}

// ===== CALL EXTERNAL API =====
// return: httpStatus, respBody(raw string), firstTxnID, allTxnIDs, error
func sendToExternalWithdrawAPI(data []byte, headers map[string]interface{}) (int, string, string, []string, error) {
	apiURL := os.Getenv("WITHDRAW_URL")
	if apiURL == "" {
		return 0, "", "", nil, errors.New("WITHDRAW_URL not set")
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	if err != nil {
		return 0, "", "", nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	authHeader := ""
	if v, ok := headers["Authorization"]; ok {
		if token, ok := v.(string); ok {
			authHeader = token
		} else if b, ok := v.([]byte); ok {
			authHeader = string(b)
		}
	}
	req.Header.Set("Authorization", authHeader)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var withdrawResp WithdrawResponse
	if err := json.Unmarshal(respBytes, &withdrawResp); err != nil {
		log.Fatal("Cannot parse response:", err)
	}

	// รวบรวม txn IDs
	txnIDs := make([]string, 0, len(withdrawResp.Data.Details))
	for _, d := range withdrawResp.Data.Details {
		if d.TransactionID != "" {
			txnIDs = append(txnIDs, d.TransactionID)
		}
	}
	firstTxnID := ""
	if len(txnIDs) > 0 {
		firstTxnID = txnIDs[0]
	}

	// แปลง struct -> JSON string สำหรับ return
	respJSON, err := json.Marshal(withdrawResp)
	if err != nil {
		return resp.StatusCode, string(respBytes), "", nil, err
	}

	return resp.StatusCode, string(respJSON), firstTxnID, txnIDs, nil
}

// ===== RPC CONSUMER =====

// ConnectMQ / CloseMQ ควรมีในโปรเจกต์ของคุณอยู่แล้ว
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
		// map headers จาก RabbitMQ ไป HTTP header
		headers := map[string]interface{}{}
		for k, v := range d.Headers {
			headers[k] = v
		}

		httpStatus, respBody, firstTxnID, txnIDs, sendErr := sendToExternalWithdrawAPI(d.Body, headers)

		log.Printf("HTTP Status: %d", httpStatus)
		log.Printf("First Transaction ID: %s", firstTxnID)
		log.Printf("All Transaction IDs: %v", txnIDs)
		log.Printf("Body length: %d", len(respBody))

		status := "sent"
		errMsg := ""
		if sendErr != nil || httpStatus >= 500 {
			status = "failed"
			if sendErr != nil {
				errMsg = sendErr.Error()
			}
		}

		// ✅ ใส่ firstTxnID ให้ตรง signature insertWithdrawLog
		_, _ = insertWithdrawLog(r.QueueName, d.Body, headers, httpStatus, respBody, status, 1, errMsg, firstTxnID)

		// ตอบกลับ RPC
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

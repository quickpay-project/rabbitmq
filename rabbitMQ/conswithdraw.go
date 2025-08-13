package rabbitmqconnect

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
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

type DepositRequest struct {
	Amount          float64 `json:"amount"`
	MID             string  `json:"mid"`
	CustomerOrderID string  `json:"customer_order_id"`
	CallbackURL     string  `json:"callback_url"`
}

type DepositResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Order struct {
			OperatorOrderID string      `json:"operator_order_id"`
			CustomerOrderID string      `json:"customer_order_id"`
			QRType          string      `json:"qr_type"`
			Amount          float64     `json:"amount"`
			AccountNumber   string      `json:"account_number"`
			AccountName     string      `json:"account_name"`
			TotalQRCode     int         `json:"total_qr_code"`
			QRDetails       interface{} `json:"qr_details"`
			BankCode        string      `json:"bank_code"`
			CallbackURL     interface{} `json:"callback_url"`
		} `json:"order"`
		Details []struct {
			TransactionID   string      `json:"transaction_id"`
			QRString        string      `json:"qr_string"`
			Amount          float64     `json:"amount"`
			NetAmount       float64     `json:"net_amount"`
			CreatedAt       string      `json:"created_at"`
			ExpiredAt       string      `json:"expired_at"`
			ImageURL        string      `json:"image_url"`
			BankCode        string      `json:"bank_code"`
			AccountName     string      `json:"account_name"`
			AccountNumber   string      `json:"account_number"`
			CustomerOrderID interface{} `json:"customer_order_id"`
			UpdatedAt       interface{} `json:"updated_at"`
			MdrAmount       interface{} `json:"mdr_amount"`
			FeeAmount       interface{} `json:"fee_amount"`
			VATAmount       interface{} `json:"vat_amount"`
			WHTAmount       interface{} `json:"wht_amount"`
		} `json:"details"`
	} `json:"data"`
}

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
		return 0, "", "", nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", "", nil, err
	}

	outerBodyStr := string(respBytes)
	log.Printf("📥 Outer body: %s", outerBodyStr)

	// parse outer JSON
	var outer map[string]interface{}
	if err := json.Unmarshal(respBytes, &outer); err != nil {
		return resp.StatusCode, outerBodyStr, "", nil, err
	}

	bodyStr, _ := outer["body"].(string)
	decoded, err := decodeBody(bodyStr)
	if err != nil {
		// ถ้า decode ไม่สำเร็จ ใช้ raw body
		decoded = []byte(bodyStr)
	}

	innerBodyStr := string(decoded)
	log.Printf("📥 Decoded inner body: %s", innerBodyStr)

	// parse inner JSON
	var inner map[string]interface{}
	if err := json.Unmarshal(decoded, &inner); err != nil {
		return resp.StatusCode, innerBodyStr, "", nil, err
	}

	// ดึง transaction_id ทุกตัวจาก details array
	txnIDs := []string{}
	if dataMap, ok := inner["data"].(map[string]interface{}); ok {
		if details, ok := dataMap["details"].([]interface{}); ok {
			for _, d := range details {
				if detail, ok := d.(map[string]interface{}); ok {
					if id, ok := detail["transaction_id"].(string); ok {
						txnIDs = append(txnIDs, id)
					}
				}
			}
		}
	}

	// transaction ตัวแรก สำหรับ insertWithdrawLog
	firstTxnID := ""
	if len(txnIDs) > 0 {
		firstTxnID = txnIDs[0]
	}

	return resp.StatusCode, innerBodyStr, firstTxnID, txnIDs, nil
}

// decodeBody รองรับ base64 + gzip
func decodeBody(bodyStr string) ([]byte, error) {
	decoded := []byte(bodyStr)

	// ลอง base64 decode
	if b, err := base64.StdEncoding.DecodeString(bodyStr); err == nil {
		decoded = b
	}

	// ลอง gzip decompress
	if gzReader, err := gzip.NewReader(bytes.NewReader(decoded)); err == nil {
		defer gzReader.Close()
		if data, err := io.ReadAll(gzReader); err == nil {
			decoded = data
		}
	}

	return decoded, nil
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

		_, _ = insertWithdrawLog(r.QueueName, d.Body, headers, httpStatus, respBody, status, 1, errMsg, firstTxnID)

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

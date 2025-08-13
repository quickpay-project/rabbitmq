package rabbitmqconnect

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
)

type RabbitMQ struct {
	Body      string
	QueueName string
}

func (r *RabbitWithdrawMQ) Puplish() (data []byte) {

	apiURL := os.Getenv("WITHDRAW_URL")

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(data))
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjIwNjkxMjYwMTYsInVzZXJJZCI6ImRkMWNhOGNkLThkNjgtNDQzOC1hZDI4LWUxMDIwZWFhNTMwZCJ9.u-HXhWv_E1fH0gxLp_0zJix5ShzY6RkHYQqxITjJwgg")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var depositResp DepositResponse
	if err := json.Unmarshal(respBytes, &depositResp); err != nil {
		log.Fatal("Cannot parse response:", err)
	}

	// แสดงข้อมูล response
	log.Printf("HTTP Status: %d", resp.StatusCode)
	log.Printf("Message: %s", depositResp.Message)
	if len(depositResp.Data.Details) > 0 {
		log.Printf("Transaction ID: %s", depositResp.Data.Details[0].TransactionID)
		log.Printf("QR String: %s", depositResp.Data.Details[0].QRString)
	}
}

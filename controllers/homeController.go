package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	rabbitmqconnect "github.com/celalsahinaltinisik/rabbitMQ"
)

type Functions struct{}

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

func isWhitelistedIP(r *http.Request) bool {
	wl := os.Getenv("WISHLIST_IP")
	if wl == "" {
		return false
	}

	allowedIPs := strings.Split(wl, ",")

	// หาค่า IP จาก Header หรือ RemoteAddr
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.RemoteAddr
		if strings.Contains(ip, ":") {
			ip = strings.Split(ip, ":")[0]
		}
	}

	for _, allow := range allowedIPs {
		if strings.TrimSpace(allow) == ip {
			return true
		}
	}
	return false
}

func (m Functions) Home(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "home")
}

func (m Functions) Consume(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "consume")
	rabbit := rabbitmqconnect.RabbitMQ{QueueName: "defaultqueuue"}
	rabbit.Consume()
}

func (m Functions) Conswithdraw(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume withdraw")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitWithdrawMQ{QueueName: "withdraw", Headers: headers}
	rabbit.ConswithdrawRPC() // ใช้ RPC consumer
}

func (m Functions) Consdeposit(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume deposit")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitDepositMQ{QueueName: "deposit", Headers: headers}
	rabbit.ConsdepositRPC() // ใช้ RPC consumer
}

func (m Functions) Publish(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	apiURL := os.Getenv("WITHDRAW_URL")

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ")

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

func (m Functions) Withdraw(w http.ResponseWriter, r *http.Request) {
	// ✅ check IP ก่อน
	if !isWhitelistedIP(r) {
		http.Error(w, "Forbidden: IP not allowed", http.StatusForbidden)
		return
	}

	body, _ := io.ReadAll(r.Body)
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitWithdrawMQ{
		Body:      string(body),
		QueueName: "withdraw",
		Headers:   headers,
	}

	response, err := rabbit.WithdrawRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (m Functions) Deposit(w http.ResponseWriter, r *http.Request) {
	// ✅ check IP ก่อน
	if !isWhitelistedIP(r) {
		http.Error(w, "Forbidden: IP not allowed", http.StatusForbidden)
		return
	}

	body, _ := io.ReadAll(r.Body)
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitDepositMQ{
		Body:      string(body),
		QueueName: "deposit",
		Headers:   headers,
	}

	response, err := rabbit.DepositRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

/*
func (m Functions) Withdraw(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitWithdrawMQ{
		Body:      string(body),
		QueueName: "withdraw",
		Headers:   headers,
	}

	response, err := rabbit.WithdrawRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (m Functions) Deposit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitDepositMQ{
		Body:      string(body),
		QueueName: "deposit",
		Headers:   headers,
	}

	response, err := rabbit.DepositRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}
*/

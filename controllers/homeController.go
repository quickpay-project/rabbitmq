package controller

import (
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

	// ✅ ดึงค่า IP
	ip := r.Header.Get("X-Forwarded-For")
	if ip != "" {
		// ตัดเอาเฉพาะตัวแรก
		ip = strings.Split(ip, ",")[0]
	} else {
		ip = r.RemoteAddr
		if strings.Contains(ip, ":") {
			ip = strings.Split(ip, ":")[0]
		}
	}

	ip = strings.TrimSpace(ip)
	log.Printf("🌐 Client IP: %s", ip)

	for _, allow := range allowedIPs {
		if strings.TrimSpace(allow) == ip {
			return true
		}
	}
	return false
}

// ✅ ฟังก์ชันตรวจสอบ group status
func checkGroupAllowed(r *http.Request, envKey string) bool {
	groupHeader := r.Header.Get("Group")
	if groupHeader == "" {
		return false
	}

	envValue := os.Getenv(envKey)
	if envValue == "" {
		return false
	}

	groupMap := make(map[string]string)
	pairs := strings.Split(envValue, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			groupMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	status, ok := groupMap[groupHeader]
	if !ok {
		return false
	}

	return status == "1"
}

func (m Functions) Home(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "enable service mq")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit_withdraw := rabbitmqconnect.RabbitWithdrawMQ{QueueName: "withdraw", Headers: headers}
	rabbit_withdraw.ConswithdrawRPC() // เปิดใช้งาน service withdraw

	rabbit_deposit := rabbitmqconnect.RabbitDepositMQ{QueueName: "deposit", Headers: headers}
	rabbit_deposit.ConsdepositRPC() // เปิดใช้งาน service deposit

	rabbit_balance := rabbitmqconnect.RabbitBalanceMQ{QueueName: "balance", Headers: headers}
	rabbit_balance.ConsbalanceRPC() // เปิดใช้งาน service balance

	rabbit_confirmorder := rabbitmqconnect.RabbitConfirmorderMQ{QueueName: "confirmorder1", Headers: headers}
	rabbit_confirmorder.ConsconfirmorderRPC() // เปิดใช้งาน service confirmorder 1

	rabbit_confirmordertwo := rabbitmqconnect.RabbitConfirmordertwoMQ{QueueName: "confirmorder2", Headers: headers}
	rabbit_confirmordertwo.ConsconfirmordertwoRPC() // เปิดใช้งาน service confirmorder 2

	rabbit_confirmorderthree := rabbitmqconnect.RabbitConfirmorderthreeMQ{QueueName: "confirmorder3", Headers: headers}
	rabbit_confirmorderthree.ConsconfirmorderthreeRPC() // เปิดใช้งาน service confirmorder 3
}

func (m Functions) Online(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "enable service mq")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit_withdraw := rabbitmqconnect.RabbitWithdrawMQ{QueueName: "withdraw", Headers: headers}
	rabbit_withdraw.ConswithdrawRPC() // เปิดใช้งาน service withdraw

	rabbit_deposit := rabbitmqconnect.RabbitDepositMQ{QueueName: "deposit", Headers: headers}
	rabbit_deposit.ConsdepositRPC() // เปิดใช้งาน service deposit

	rabbit_balance := rabbitmqconnect.RabbitBalanceMQ{QueueName: "balance", Headers: headers}
	rabbit_balance.ConsbalanceRPC() // เปิดใช้งาน service balance

	rabbit_confirmorder := rabbitmqconnect.RabbitConfirmorderMQ{QueueName: "confirmorder1", Headers: headers}
	rabbit_confirmorder.ConsconfirmorderRPC() // เปิดใช้งาน service confirmorder 1

	rabbit_confirmordertwo := rabbitmqconnect.RabbitConfirmordertwoMQ{QueueName: "confirmorder2", Headers: headers}
	rabbit_confirmordertwo.ConsconfirmordertwoRPC() // เปิดใช้งาน service confirmorder 2

	rabbit_confirmorderthree := rabbitmqconnect.RabbitConfirmorderthreeMQ{QueueName: "confirmorder3", Headers: headers}
	rabbit_confirmorderthree.ConsconfirmorderthreeRPC() // เปิดใช้งาน service confirmorder 3
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

func (m Functions) Consbalance(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume balance")

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitBalanceMQ{QueueName: "balance", Headers: headers}
	rabbit.ConsbalanceRPC()
}

func (m Functions) Consconfirmorder(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume confirmorder")

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitConfirmorderMQ{QueueName: "confirmorder1", Headers: headers}
	rabbit.ConsconfirmorderRPC()
}

func (m Functions) Consconfirmordertwo(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume confirmorder 2")

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitConfirmordertwoMQ{QueueName: "confirmorder2", Headers: headers}
	rabbit.ConsconfirmordertwoRPC()
}

func (m Functions) Consconfirmorderthree(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Consume confirmorder 2")

	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	rabbit := rabbitmqconnect.RabbitConfirmorderthreeMQ{QueueName: "confirmorder3", Headers: headers}
	rabbit.ConsconfirmorderthreeRPC()
}

func (m Functions) Withdraw(w http.ResponseWriter, r *http.Request) {
	// ✅ check IP ก่อน
	if !isWhitelistedIP(r) {
		http.Error(w, "Forbidden: IP not allowed", http.StatusForbidden)
		return
	}

	// ✅ check group ก่อน
	if !checkGroupAllowed(r, "WITHDRAW_GROUP_STATUS") {
		http.Error(w, "Forbidden: Withdraw not allowed or closed", http.StatusForbidden)
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

	// ✅ check group ก่อน
	if !checkGroupAllowed(r, "DEPOSIT_GROUP_STATUS") {
		http.Error(w, "Forbidden: Deposit not allowed or closed", http.StatusForbidden)
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

func (m Functions) Confirmorder(w http.ResponseWriter, r *http.Request) {
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

	rabbit := rabbitmqconnect.RabbitConfirmorderMQ{
		Body:      string(body),
		QueueName: "confirmorder1",
		Headers:   headers,
	}

	response, err := rabbit.ConfirmorderRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (m Functions) Confirmordertwo(w http.ResponseWriter, r *http.Request) {
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

	rabbit := rabbitmqconnect.RabbitConfirmordertwoMQ{
		Body:      string(body),
		QueueName: "confirmorder2",
		Headers:   headers,
	}

	response, err := rabbit.ConfirmordertwoRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (m Functions) Confirmorderthree(w http.ResponseWriter, r *http.Request) {
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

	rabbit := rabbitmqconnect.RabbitConfirmorderthreeMQ{
		Body:      string(body),
		QueueName: "confirmorder3",
		Headers:   headers,
	}

	response, err := rabbit.ConfirmorderthreeRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

// =======================
// /balance
// =======================
func (m Functions) Balance(w http.ResponseWriter, r *http.Request) {
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

	rabbit := rabbitmqconnect.RabbitBalanceMQ{
		Body:      string(body),
		QueueName: "balance",
		Headers:   headers,
	}

	response, err := rabbit.BalanceRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

// =======================
// /merchant-keys/update-balance
// =======================
func (m Functions) Merchantkeysupdatebalance(w http.ResponseWriter, r *http.Request) {
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

	rabbit := rabbitmqconnect.RabbitBalanceMQ{
		Body:      string(body),
		QueueName: "balance",
		Headers:   headers,
	}

	response, err := rabbit.BalanceRPC()
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

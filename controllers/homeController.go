package controller

import (
	"fmt"
	"io"
	"log"
	"net/http"

	errors "github.com/celalsahinaltinisik/exceptions"
	rabbitmqconnect "github.com/celalsahinaltinisik/rabbitMQ"
)

type Functions struct{}

func (m Functions) Home(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "home")
	log.Println(r.Body)
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

	log.Printf(" [x] Sent Headers %s\n", headers)

	rabbit := rabbitmqconnect.RabbitWithdrawMQ{QueueName: "withdraw", Headers: headers}
	rabbit.Conswithdraw()
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
	rabbit.Consdeposit()
}

func (m Functions) Publish(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	errors.FailOnError(err, "Failed to readall body request")
	rabbit := rabbitmqconnect.RabbitMQ{Body: string(body), QueueName: "defaultqueuue"}
	rabbit.Puplish()
}

func (m Functions) Withdraw(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	errors.FailOnError(err, "Failed to readall body request")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	log.Printf(" [x] Sent Headers %s\n", headers)

	rabbit := rabbitmqconnect.RabbitWithdrawMQ{Body: string(body), QueueName: "withdraw", Headers: headers}
	rabbit.Withdraw()
}

func (m Functions) Deposit(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	errors.FailOnError(err, "Failed to readall body request")

	// อ่าน Header
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	log.Printf(" [x] Sent Headers %s\n", headers)

	rabbit := rabbitmqconnect.RabbitDepositMQ{Body: string(body), QueueName: "deposit", Headers: headers}
	rabbit.Deposit()
}

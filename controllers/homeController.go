package controller

import (
	"fmt"
	"io"
	"log"
	"net/http"

	errors "github.com/celalsahinaltinisik/exceptions"
	rabbitmqconnect "github.com/celalsahinaltinisik/rabbitMQ"
)

// type request struct {
// 	Name string
// 	Age  int
// 	Car  string
// }

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

func (m Functions) Publish(w http.ResponseWriter, r *http.Request) {

	body, err := io.ReadAll(r.Body)
	errors.FailOnError(err, "Failed to readall body request")
	rabbit := rabbitmqconnect.RabbitMQ{Body: string(body), QueueName: "defaultqueuue"}
	rabbit.Puplish()
	// fmt.Fprintln(w, "home")
	// request := request{}
	// decoder := json.NewDecoder(r.Body)
	// err := decoder.Decode(&request)
	// if err != nil {
	// 	panic(err)
	// }
	// log.Println(request)
}

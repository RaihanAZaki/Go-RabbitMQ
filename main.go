package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
)

func main() {
	//connect to rabbitMQ
	conn, err := amqp.Dial("amqp://guest:rehan@localhost:5672/")
	if err != nil {
		fmt.Println("err connect", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println("Error closing connection", err)
		}
	}()

	fmt.Println("Successfully connected to rabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel", err)
		return
	}
	defer func() {
		if err := ch.Close(); err != nil {
			fmt.Println("Error closing channel", err)
		}
	}()

	q, err := ch.QueueDeclare(
		"TestQueue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("err queue", err)
		return
	}

	type Person struct {
		Name     string
		Age      int
		Email    string
		Password string
	}

	Rehan := Person{
		Name:     "rehan",
		Age:      20,
		Email:    "rehan@gmail.com",
		Password: "123",
	}

	Rifqi := Person{
		Name:     "Rifqi",
		Age:      20,
		Email:    "rifqi@gmail.com",
		Password: "123",
	}

	dataArray := []Person{Rehan, Rifqi}

	DataJson, err := json.MarshalIndent(dataArray, "", "  ")
	if err != nil {
		fmt.Println("err marshal", err)
		return
	}

	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        DataJson,
		},
	)
	if err != nil {
		fmt.Println("err publish", err)
		return
	}
	fmt.Println("Successfully publish message to Queue")
}

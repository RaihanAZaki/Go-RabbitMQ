package main

import (
	"fmt"
	"github.com/streadway/amqp"
)

func main() {
	fmt.Println("Consumer Application")

	conn, err := amqp.Dial("amqp://guest:rehan@localhost:5672")
	if err != nil {
		fmt.Println("err connection :", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to rabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("err channel :", err)
	}
	defer ch.Close ()

	msgs, err := ch.Consume(
		"TestQueue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			fmt.Printf("Receive : %s \n", d.Body)
		}
	}()

	fmt.Println("[*] Waiting Message .......")

	<- forever
}
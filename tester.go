package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/streadway/amqp"
	_ "github.com/go-sql-driver/mysql"
)

type Person struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

func main() {
	// Connect to MySQL
	engine, err := xorm.NewEngine("mysql", "root:rehan@tcp(localhost:7000)/atms")
	if err != nil {
		log.Fatal("Error connecting to MySQL:", err)
	}
	defer db.Close()

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:rehan@localhost:5672/")
	if err != nil {
		log.Fatal("Error connecting to RabbitMQ:", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to both MySQL and RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Error creating channel", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"TestQueue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Error declaring queue", err)
	}

	// Fetch data from MySQL
	rows, err := db.Query("SELECT id, username, email, password FROM user")
	if err != nil {
		log.Fatal("Error querying MySQL:", err)
	}
	defer rows.Close()

	var people []Person

	// Iterate through the rows and populate the people slice
	for rows.Next() {
		var person Person
		err := rows.Scan(&person.ID, &person.Username, &person.Email, &person.Password)
		if err != nil {
			log.Fatal("Error scanning row:", err)
		}
		people = append(people, person)
	}

	// Marshal the data to JSON
	dataJson, err := json.MarshalIndent(people, "", "  ")
	if err != nil {
		log.Fatal("Error marshalling to JSON:", err)
	}

	// Publish data to RabbitMQ
	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        dataJson,
		},
	)
	if err != nil {
		log.Fatal("Error publishing to RabbitMQ:", err)
	}
	fmt.Println("Successfully published message to Queue")

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("Shutting down gracefully...")
}

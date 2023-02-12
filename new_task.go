package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/loveyandex/TaskQueuesRmq/worke"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}


func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	failOnError(err, "Failed to publish a message")

	app := fiber.New()
	app.Use(cors.New())

	app.Get("/", func(c *fiber.Ctx) error {
		var limitOrder worke.OrderBook
		err := c.QueryParser(&limitOrder)

		b, err := json.Marshal(limitOrder)

		if err != nil {
			return err
		}

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})
		// log.Printf(" [x] Sent %s", b)

		return c.SendStatus(fiber.StatusOK)
	})

	app.Post("/", func(c *fiber.Ctx) error {
		var limitOrder worke.OrderBook
		err := c.BodyParser(&limitOrder)
		
		if err != nil {
			return err
		}

		b, err := json.Marshal(limitOrder)

		if err != nil {
			return err
		}

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})
		// log.Printf(" [x] Sent %s", b)

		return c.SendStatus(fiber.StatusOK)
	})

	app.Listen(":4000")

}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/websocket/v2"

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

	qme, err := ch.QueueDeclare(
		"me",  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ch2, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch2.Close()

	err = ch2.ExchangeDeclare(
		"orderbooks", // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

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
		failOnError(err, "Failed to publish a message")
		// log.Printf(" [x] Sent %s", b)

		return c.SendStatus(fiber.StatusOK)
	})

	app.Get("/me", func(c *fiber.Ctx) error {
		var limitOrder worke.OrderBook
		err := c.QueryParser(&limitOrder)

		b, err := json.Marshal(limitOrder)
		failOnError(err, "new error")

		if err != nil {
			return err
		}

		err = ch.Publish(
			"",       // exchange
			qme.Name, // routing key
			false,    // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})
		// log.Printf(" [x] Sent %s", b)
		failOnError(err, "Failed to publish a message")

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
	app.Post("/me", func(c *fiber.Ctx) error {
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
			"",       // exchange
			qme.Name, // routing key
			false,    // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})

		if err != nil {
			return err
		}
		// log.Printf(" [x] Sent %s", b)

		return c.SendStatus(fiber.StatusOK)
	})

	app.Use("/ws", func(c *fiber.Ctx) error {
		// IsWebSocketUpgrade returns true if the client
		// requested upgrade to the WebSocket protocol.
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	// orderbooksQ, err := ch.QueueDeclare(
	// 	"orderbooks", // name
	// 	true,         // durable
	// 	false,        // delete when unused
	// 	false,        // exclusive
	// 	false,        // no-wait
	// 	nil,          // arguments
	// )
	// if err != nil {
	// 	failOnError(err, "orderbook wuere declaring error")
	// }

	// orderbooks, err := ch.Consume(
	// 	orderbooksQ.Name, // queue
	// 	"",               // consumer
	// 	false,            // auto-ack
	// 	false,            // exclusive
	// 	false,            // no-local
	// 	false,            // no-wait
	// 	nil,              // args
	// )

	app.Get("/ws/:id", websocket.New(func(c *websocket.Conn) {
		// c.Locals is added to the *websocket.Conn
		log.Println(c.Locals("allowed"))  // true
		log.Println(c.Params("id"))       // 123
		log.Println(c.Query("v"))         // 1.0
		log.Println(c.Cookies("session")) // ""

		q, err := ch2.QueueDeclare(
			"",    // name
			false, // durable
			false, // delete when unused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")

		err = ch2.QueueBind(
			q.Name,       // queue name
			"",           // routing key
			"orderbooks", // exchange
			false,
			nil,
		)
		failOnError(err, "Failed to bind a queue")

		msgs, err := ch2.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")

		var forever chan struct{}

		go func() {
			for d := range msgs {
				if err = c.WriteMessage(1, d.Body); err != nil {
					log.Println("orderbooks write:", err)

				}

			}
		}()

		log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
		<-forever

		// var forever chan string
		// c.SetCloseHandler(func(code int, text string) error {
		// 	fmt.Printf("text: %v\n", text)
		// 	forever <- text
		// 	return nil
		// })

		// for d := range orderbooks {
		// 	b := d.Body
		// 	// log.Printf("orderbooks recv: %s", b)

		// 	if err = c.WriteMessage(1, b); err != nil {
		// 		log.Println("orderbooks write:", err)

		// 	}
		// }
		// <-forever

	}))

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

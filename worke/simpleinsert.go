package worke

import (
	"encoding/json"
	"fmt"
	"log"

	// worke "github.com/loveyandex/TaskQueuesRmq/worke/db"
	amqp "github.com/rabbitmq/amqp091-go"
	//"go.mongodb.org/mongo-driver/bson"
)

func InsertSimple() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

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

	q, err := ch.QueueDeclare(
		"me",  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	var forever chan struct{}

	engine := &MongoME{Col: Collection("engine")}
	fmt.Printf("engine: %v\n", engine)

	obc := &OrderBookCollection{Col: Collection("orders")}
	fmt.Printf("obc: %v\n", obc)

	go func() {
		for d := range msgs {
			// log.Printf("Received a message: %s %v", d.Body, time.Now())
			var newOrder OrderBook
			json.Unmarshal(d.Body, &newOrder)
			newOrder.Trades = []Trade{}
			newOrder.Status = Open

			// _, err2 := worke.UserCollection.InsertOne(context.Background(), newOrder)

			// if err2 != nil {

			// }
			// // fmt.Printf("ior: %v\n", ior)

			// _, err := worke.UserCollection.CountDocuments(context.Background(), bson.M{})
			// if err != nil {
			// }
			// fmt.Printf("count: %v\n", i)

			err = ch.PublishWithContext(ctx,
				"orderbooks", // exchange
				"",           // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(d.Body),
				})
			failOnError(err, "Failed to publish a message")
			//      fmt.Printf("time.Now(): %v\n", time.Now())

			// log.Printf("%v", *res)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

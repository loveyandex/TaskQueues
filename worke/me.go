package worke

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func MongoMOE() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

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

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s %v", d.Body, time.Now())
			longers, err2 := obc.BuyLimitOrders()
			if err2 != nil {

			}
			fmt.Printf("ob: %v\n", len(longers))

			shorts, err2 := obc.ShortLimitOrders()
			if err2 != nil {

			}
				fmt.Printf("shorts len: %v\n", len(shorts))
			obc.MATCH(shorts, longers)

			// log.Printf("%v", *res)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (obe *OrderBookCollection) MATCH(shorts []OrderBook, longs []OrderBook) {
	//fill buy order books by possible shorters
	 
	for _, ashort := range shorts {

	xxxx:
		for _, along := range longs {

			if ashort.Price <= along.Price {
				fmt.Printf("ashort: %v\n", ashort)
				possible_short_amount := (ashort.Amount - ashort.FillAmount)
				possible_long_amount := (along.Amount - along.FillAmount)

				if (possible_long_amount - possible_short_amount) >= 0 {
					fmt.Printf("along: %v\n", along)

					//in this model short filled all
					ashort.FillAmount = ashort.FillAmount + possible_short_amount
					trd := &Trade{
						ID: primitive.NewObjectID(),
						Symbol: along.Symbol,
						Price:  along.Price,
						Amount: possible_short_amount,
						Type:   ashort.Type,
						Side: ashort.Side,
					}
					obe.UpdateOrderFillamountAndStatus(&ashort, Fill, trd)

					along.FillAmount = along.FillAmount + (possible_short_amount)

					if along.FillAmount == along.Amount {
						obe.UpdateOrderFillamountAndStatus(&along, Fill, trd)
					} else {
						obe.UpdateOrderFillamountAndStatus(&along, PartiallyFilled, trd)
					}
					return
					break xxxx

				} else {
					trd := &Trade{
						ID: primitive.NewObjectID(),
						Symbol: along.Symbol,
						Price:  along.Price,
						Amount: possible_long_amount,
						Type:   along.Type,
						Side: along.Side,
					}
					//in this model short filled all
					along.FillAmount = along.FillAmount + possible_long_amount
					obe.UpdateOrderFillamountAndStatus(&along, Fill, trd)

					ashort.FillAmount = ashort.FillAmount + (possible_long_amount)

					if ashort.FillAmount == ashort.Amount {
						obe.UpdateOrderFillamountAndStatus(&ashort, Fill, trd)
					} else {
						obe.UpdateOrderFillamountAndStatus(&ashort, PartiallyFilled, trd)
					}
					return

				}

			}

		}

	}

}

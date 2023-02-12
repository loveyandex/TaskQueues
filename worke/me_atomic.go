package worke

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func MongoMOE_Atomic() {

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
			var newOrder OrderBook
			json.Unmarshal(d.Body, &newOrder)
			newOrder.Trades = []Trade{}
			newOrder.Status = Open

			obc.MATCH_Atomic(&newOrder)

			// log.Printf("%v", *res)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (obe *OrderBookCollection) MATCH_Atomic(neworder *OrderBook) error {

	if neworder.Side == "sell" {
		obe.Cusume_New_Sell_Order__(neworder)
	} else if neworder.Side == "buy" {
		obe.Cusume_New_Buy_Order__(neworder)
	}

	return nil
}

func (obe *OrderBookCollection) Cusume_New_Sell_Order__(ashort *OrderBook) error {

	irrrr, err := obe.InsertOrder(ashort)
	if err != nil {
		panic(err)
	}
	ashort.ID = irrrr.InsertedID.(primitive.ObjectID)

	longers, err := obe.BuyLimitOrders()
	if err != nil {
		return err
	}
	for _, along := range longers {

		if ashort.Price <= along.Price {
			fmt.Printf("ashort: %v\n", ashort)
			possible_short_amount := (ashort.Amount - ashort.FillAmount)
			possible_long_amount := (along.Amount - along.FillAmount)

			if (possible_long_amount - possible_short_amount) >= 0 {
				fmt.Printf("along: %v\n", along)

				//in this model short filled all
				ashort.FillAmount = ashort.FillAmount + possible_short_amount
				trd := &Trade{
					ID:     primitive.NewObjectID(),
					Symbol: along.Symbol,
					Price:  along.Price,
					Amount: possible_short_amount,
					Type:   ashort.Type,
					Side:   ashort.Side,
				}
				_, err := obe.UpdateOrderFillamountAndStatus(ashort, Fill, trd)
				if err != nil {
					return err
				}

				along.FillAmount = along.FillAmount + (possible_short_amount)

				if along.FillAmount == along.Amount {
					_, err := obe.UpdateOrderFillamountAndStatus(&along, Fill, trd)
					if err != nil {
						return err
					}
				} else {
					_, err := obe.UpdateOrderFillamountAndStatus(&along, PartiallyFilled, trd)
					if err != nil {
						return err
					}
				}
				break

			} else {
				trd := &Trade{
					ID:     primitive.NewObjectID(),
					Symbol: along.Symbol,
					Price:  along.Price,
					Amount: possible_long_amount,
					Type:   along.Type,
					Side:   along.Side,
				}
				//in this model short filled all
				along.FillAmount = along.FillAmount + possible_long_amount
				_, err := obe.UpdateOrderFillamountAndStatus(&along, Fill, trd)
				if err != nil {
					return err
				}

				ashort.FillAmount = ashort.FillAmount + (possible_long_amount)

				if ashort.FillAmount == ashort.Amount {
					_, err := obe.UpdateOrderFillamountAndStatus(ashort, Fill, trd)
					if err != nil {
						return err
					}
				} else {
					_, err := obe.UpdateOrderFillamountAndStatus(ashort, PartiallyFilled, trd)
					if err != nil {
						return err
					}
				}

			}

		}

	}

	return nil

}

func (obe *OrderBookCollection) Cusume_New_Buy_Order__(along *OrderBook) error {

	irrrr, err := obe.InsertOrder(along)
	if err != nil {
		panic(err)
	}
	along.ID = irrrr.InsertedID.(primitive.ObjectID)

	shoerts, err := obe.ShortLimitOrders()
	if err != nil {
		return err
	}
	for _, shoert := range shoerts {

		if along.Price >= shoert.Price {
			fmt.Printf("Cusume_New_Buy_Order__ along: %v\n", along)
			possible_short_amount := (shoert.Amount - shoert.FillAmount)
			possible_long_amount := (along.Amount - along.FillAmount)

			if (possible_short_amount - possible_long_amount) >= 0 {
				fmt.Printf("Cusume_New_Buy_Order__ short: %v\n", shoert)

				//in this model long filled all
				along.FillAmount = along.FillAmount + possible_long_amount
				trd := &Trade{
					ID:     primitive.NewObjectID(),
					Symbol: along.Symbol,
					Price:  shoert.Price,
					Amount: possible_long_amount,
					Type:   along.Type,
					Side:   along.Side,
				}
				_, err := obe.UpdateOrderFillamountAndStatus(along, Fill, trd)
				if err != nil {
					return err
				}

				shoert.FillAmount = shoert.FillAmount + (possible_long_amount)

				if along.FillAmount == along.Amount {
					_, err := obe.UpdateOrderFillamountAndStatus(&shoert, Fill, trd)
					if err != nil {
						return err
					}
				} else {
					_, err := obe.UpdateOrderFillamountAndStatus(&shoert, PartiallyFilled, trd)
					if err != nil {
						return err
					}
				}
				break

			} else {
				trd := &Trade{
					ID:     primitive.NewObjectID(),
					Symbol: along.Symbol,
					Price:  shoert.Price,
					Amount: possible_short_amount,
					Type:  shoert.Type,
					Side:   shoert.Side,
				}
				//in this model long filled all
				shoert.FillAmount = shoert.FillAmount + possible_short_amount
				_, err := obe.UpdateOrderFillamountAndStatus(&shoert, Fill, trd)
				if err != nil {
					return err
				}

				along.FillAmount = along.FillAmount + (possible_short_amount)

				if along.FillAmount == along.Amount {
					_, err := obe.UpdateOrderFillamountAndStatus(along, Fill, trd)
					if err != nil {
						return err
					}
				} else {
					_, err := obe.UpdateOrderFillamountAndStatus(along, PartiallyFilled, trd)
					if err != nil {
						return err
					}
				}

			}

		}

	}

	return nil

}

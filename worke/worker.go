package worke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	worke "github.com/loveyandex/TaskQueuesRmq/worke/db"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type OrderBook struct {
	ID       primitive.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	TraderId primitive.ObjectID `json:"trader_id" bson:"trader_id"`
	//send by trader
	Symbol string
	Price  float64
	Amount float64
	Type   string
	Side   string
	//set bby M E
	FillAmount float64 `bson:"fill_amount,omitempty" json:"fill_amount,omitempty"`
	Status     OrderStatus

	//each order make many trades
	Trades []Trade 
}

type OrderStatus string

const (
	Open            OrderStatus = "open"
	Fill            OrderStatus = "filled"
	PartiallyFilled OrderStatus = "partiallyfilled"
	Failed          OrderStatus = "failed"
	Canceled        OrderStatus = "canceld"
)
type Trade struct {

	ID       primitive.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
 	//send by trader
	Symbol string
	Price  float64
	Amount float64
	Type   string
	Side   string
}



func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func Cpu() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

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

	engine := &MacEngine{Client: rdb}
	fmt.Printf("engine: %v\n", engine)

	// err = rdb.Set(ctx, "limits", "[]", 0).Err()
	// err = rdb.Set(ctx, "markets", "[]", 0).Err()
	// if err != nil {
	// 	panic(err)
	// }

	go func() {
		for d := range msgs {
			// log.Printf("Received a message: %s %v", d.Body, time.Now())
			var newOrder OrderBook
			json.Unmarshal(d.Body, &newOrder)

			// if newOrder.Type == "" {
			// 	ob, _ := engine.LimitOrders()
			// 	fmt.Printf("ob: %v\n", len(ob))

			// 	engine.PushLimitOrder(&newOrder)
			// 	// fmt.Printf("ob2: %v\n", len(ob2))

			// } else {

			// 	ob, _ := engine.MarketOrders()
			// 	fmt.Printf("ob: %v\n", len(ob))
			// 	limits, _ := engine.LimitOrders()
			// 	fmt.Printf("limits len: %v\n", len(limits))

			// 	ob2, _ := engine.PushMarketOrder(&newOrder)
			// 	fmt.Printf("markets len: %v\n", len(ob2))

			// }

			if newOrder.Type == "limit" {

				i, _ := worke.UserCollection.CountDocuments(context.TODO(), bson.M{})
				fmt.Printf("usrs len: %v\n", i)

			}
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)

			worke.UserCollection.InsertOne(context.TODO(), bson.M{
				"userId":    rand.Float32(),
				"id":        rand.Int63(),
				"createdAt": time.Now(),
				"completed": true,
			})

			// log.Printf("%v", *res)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

type MacEngine struct {
	Client *redis.Client
}

func (me *MacEngine) LimitOrders() ([]OrderBook, error) {
	s, err := me.Client.Get(ctx, "limits").Result()
	if err != nil {
		return nil, err
	}
	var ob []OrderBook
	err = json.Unmarshal([]byte(s), &ob)

	return ob, err

}

func (me *MacEngine) MarketOrders() ([]OrderBook, error) {
	s, err := me.Client.Get(ctx, "markets").Result()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return nil, err
	}
	var ob []OrderBook
	err = json.Unmarshal([]byte(s), &ob)

	return ob, err

}

func (me *MacEngine) PushLimitOrder(ob *OrderBook) ([]OrderBook, error) {

	s, err := me.Client.Get(ctx, "limits").Result()
	if err != nil {
		return nil, err
	}
	var obs []OrderBook
	err = json.Unmarshal([]byte(s), &obs)

	if err != nil {
		return nil, err
	}

	obs = append(obs, *ob)

	b, err := json.Marshal(obs)

	if err != nil {
		return nil, err
	}

	err = me.Client.Set(ctx, "limits", string(b), 0).Err()
	if err != nil {
		panic(err)
	}

	return obs, err

}

func (me *MacEngine) PushMarketOrder(ob *OrderBook) ([]OrderBook, error) {
	s, err := me.Client.Get(ctx, "markets").Result()
	if err != nil {
		return nil, err
	}
	var obs []OrderBook
	err = json.Unmarshal([]byte(s), &obs)

	if err != nil {
		return nil, err
	}

	obs = append(obs, *ob)

	b, err := json.Marshal(obs)

	if err != nil {
		return nil, err
	}

	err = me.Client.Set(ctx, "markets", string(b), 0).Err()
	if err != nil {
		panic(err)
	}

	return obs, err

}

package worke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	worke "github.com/loveyandex/TaskQueuesRmq/worke/db"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const DBName = "matching-engine"

//GetMongoDbConnection get connection of mongodb
var Client, err = GetMongoDbConnection()

func Collection(CollectionName string) *mongo.Collection {
	// fmt.Printf("robinClient %v \n", robinClient)
	collection := Client.Database(DBName).Collection(CollectionName)
	return collection
}

func GetMongoDbConnection() (*mongo.Client, error) {

	dbserver := os.Getenv("MONGODBHOST")
	URIII := "mongodb://" + dbserver + "/?replicaSet=myReplicaSet"
	if dbserver == "" {
		dbserver = "localhost"
		URIII = "mongodb://" + dbserver + ":27017/"

	}
	fmt.Println("dbserver ", dbserver)
	fmt.Println("uriii ", URIII)

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(URIII))
	// fmt.Printf("db client after connection %v ", client)

	if err != nil {
		fmt.Printf("err %v \n", err)
		log.Fatal(err)
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		fmt.Printf("err client.Ping %v \n", err)
		log.Fatal(err)
	}
	return client, nil
}

func MongoCpu() {

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

	engine := &MongoME{Col: Collection("engine")}
	fmt.Printf("engine: %v\n", engine)
	
	engine.CreateCoinEngin(&CoinEngine{Coin: &Coin{Symbol: "USDTRLS"}})

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

type MongoME struct {
	Col *mongo.Collection
}

type CoinEngine struct {

	Coin *Coin `json:"coin" bson:"coin"`
	Markets []OrderBook

	Limits []OrderBook
}
type Coin struct {
	Symbol string
}

func (me *MongoME) CoinEngin(symbol string) (*CoinEngine, error) {
	fmt.Printf("me.Col: %v\n", me.Col)
	var E CoinEngine
	err := me.Col.FindOne(context.TODO(), bson.M{"coin.symbol": symbol}).Decode(&E)

	return &E, err
}

func (me *MongoME) CreateCoinEngin(ce *CoinEngine) (*mongo.InsertOneResult, error) {
	t, err := me.Col.InsertOne(context.TODO(), ce)
	return t, err
}

func (me *MongoME) PushMarket(ce *CoinEngine, m *OrderBook) (*mongo.UpdateResult, error) {
	fmt.Printf("me.Col: %v\n", me.Col)
	filter := bson.M{"coin.symbol": ce.Coin.Symbol}
	updt := bson.M{"$push": bson.M{"markets": m}}
	t, err := me.Col.UpdateOne(context.TODO(), filter, updt)
	return t, err
}

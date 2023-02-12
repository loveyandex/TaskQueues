package worke

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const DBName = "matching-engine3"

//GetMongoDbConnection get connection of mongodb
var Client, _ = GetMongoDbConnection()

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

	obc := &OrderBookCollection{Col: Collection("orders")}

	go func() {
		for d := range msgs {
			// log.Printf("Received a message: %s %v", d.Body, time.Now())
			var newOrder OrderBook
			json.Unmarshal(d.Body, &newOrder)

			if newOrder.Type == "market" || newOrder.Type == "limit" {
				newOrder.Status = Open
				newOrder.Trades = []Trade{}
				irrrr, err := obc.InsertOrder(&newOrder)

				newOrder.ID = irrrr.InsertedID.(primitive.ObjectID)
				// newOrder.FillAmount = (rand.Float64()) * newOrder.Amount
				// obc.UpdateOrderFillamountAndStatus(&newOrder, PartiallyFilled)
				if err != nil {
					panic(err)
				}

			}

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

type OrderBookCollection struct {
	Col *mongo.Collection
}

type CoinEngine struct {
	Coin    *Coin `json:"coin" bson:"coin"`
	Markets []OrderBook

	Limits []OrderBook
}
type Coin struct {
	Symbol string
}

func (me *MongoME) CoinEngin(symbol string) (*CoinEngine, error) {
	var E CoinEngine
	err := me.Col.FindOne(context.TODO(), bson.M{"coin.symbol": symbol}).Decode(&E)

	return &E, err
}

func (me *MongoME) CreateCoinEngin(ce *CoinEngine) (*mongo.InsertOneResult, error) {
	t, err := me.Col.InsertOne(context.TODO(), ce)
	return t, err
}

func (me *MongoME) PushMarket(ce *CoinEngine, m *OrderBook) (*mongo.UpdateResult, error) {
	filter := bson.M{"coin.symbol": ce.Coin.Symbol}
	updt := bson.M{"$push": bson.M{"markets": m}}
	t, err := me.Col.UpdateOne(context.TODO(), filter, updt)
	return t, err
}

func (bc *OrderBookCollection) InsertOrder(o *OrderBook) (*mongo.InsertOneResult, error) {
	t, err := bc.Col.InsertOne(context.TODO(), o)
	return t, err
}

func (bc *OrderBookCollection) UpdateOrderStatus(o *OrderBook, os OrderStatus) (*mongo.UpdateResult, error) {

	filter := bson.M{"_id": o.ID}
	updt := bson.M{"$set": bson.M{"status": os}}

	t, err := bc.Col.UpdateOne(context.TODO(), filter, updt)
	return t, err
}

func (bc *OrderBookCollection) UpdateOrderFillamountAndStatus(o *OrderBook, os OrderStatus, trde *Trade) (*mongo.UpdateResult, error) {

	filter := bson.M{"_id": o.ID}
	updt := bson.M{"$set": bson.M{"status": os, "fill_amount": o.FillAmount}, "$push": bson.M{"trades": trde}}
	t, err := bc.Col.UpdateOne(context.TODO(), filter, updt)
	return t, err
}

func (bc *OrderBookCollection) BuyLimitOrders() ([]OrderBook, error) {

	opts := options.Find().SetSkip(0 * 1000).SetLimit(200)

	query := bson.M{
		"type": "limit",
		"side": "buy",
		"$or": bson.A{
			bson.D{{"status", "open"}},
			bson.D{{"status", "partiallyfilled"}},
		}}

	//sort by time added default
	// Sort by `price` field descending
	opts.SetSort(bson.D{{"price", -1}})
	cursor, err2 := bc.Col.Find(context.TODO(), query, opts)
	if err2 != nil {
		return nil, err2
	}
	var results []OrderBook
	f := cursor.All(context.Background(), &results)
	if f != nil {
		return nil, f
	}
	return results, nil

}
func (bc *OrderBookCollection) OrderBooks() ([]OrderBook, error) {

	opts := options.Find().SetSkip(0 * 1000).SetLimit(200)

	query := bson.M{
		"$or": bson.A{
			bson.D{{"status", "open"}},
			bson.D{{"status", "partiallyfilled"}},
		}}

	//sort by time added default
	// Sort by `price` field descending
	opts.SetSort(bson.D{{"price", -1}})
	cursor, err2 := bc.Col.Find(context.TODO(), query, opts)
	if err2 != nil {
		return nil, err2
	}
	var results []OrderBook
	f := cursor.All(context.Background(), &results)
	if f != nil {
		return nil, f
	}
	return results, nil

}
func (bc *OrderBookCollection) OrderCounts() (int64, error) {

	query := bson.M{}

	//sort by time added default
	// Sort by `price` field descending
	iii, err2 := bc.Col.CountDocuments(context.TODO(), query)

	return iii, err2

}

func (bc *OrderBookCollection) ShortLimitOrders() ([]OrderBook, error) {

	opts := options.Find().SetSkip(0 * 1000).SetLimit(20000)

	query := bson.M{
		"type": "limit",
		"side": "sell",
		"$or": bson.A{
			bson.D{{"status", "open"}},
			bson.D{{"status", "partiallyfilled"}},
		}}

	//sort by time added default
	// Sort by `price` field descending
	opts.SetSort(bson.D{{"price", 1}})

	cursor, err2 := bc.Col.Find(context.TODO(), query, opts)
	if err2 != nil {
		return nil, err2
	}
	var results []OrderBook
	f := cursor.All(context.Background(), &results)
	if f != nil {
		return nil, f
	}
	return results, nil

}

package worke

import (
	"context" 
	"log"
	"math/rand" 
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

//GetMongoDbConnection get connection of mongodb
var client, err = GetMongoDbConnection()
var client2, _ = GetMongoDbConnection()

func GetMongoDbConnection() (*mongo.Client, error) {

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27010"))
 
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	return client, nil
}

func GetMongoDbCollection(DbName string, CollectionName string) (*mongo.Collection, error) {

	if err != nil {
		return nil, err
	}

	var robinClient *mongo.Client

	if rand.Float64() < 0.5 {
		robinClient = client

	} else {

		robinClient = client2
	}

 


	collection := robinClient.Database(DbName).Collection(CollectionName)

	return collection, nil
}

package main

import "github.com/loveyandex/TaskQueuesRmq/worke"

func main() {

	go func() {
		worke.MongoMOE()

	}()
	worke.MongoCpu()
}

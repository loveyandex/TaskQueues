package main

import "github.com/loveyandex/TaskQueuesRmq/worke"

func main() {

	go func() {
		worke.MongoMOE_Atomic()

	}()
	worke.MongoCpu()
}

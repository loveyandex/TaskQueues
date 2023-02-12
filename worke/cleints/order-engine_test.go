package cleints

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/loveyandex/TaskQueuesRmq/worke"
)

func TestPushOrders(t *testing.T) {
	// for i := 0; i < 333; i++ {
	// 	MkOrdr()

	// }
	go func() {
		for {
			MkOrdr()
			time.Sleep(time.Microsecond * 1)
		}
	}()
	go func() {
		for {
			MkOrdr()
			time.Sleep(time.Microsecond * 1)
		}
	}()
	time.Sleep(time.Second)
	fmt.Println("done")

	for {
		MkOrdr()
		time.Sleep(time.Microsecond * 1)
	}

}

func MkOrdr() {

	url := "http://127.0.0.1:4000"
	method := "POST"

	side := "sell"
	if rand.Float64() > 0.5 {
		side = "buy"
	}
	typp := "limit"
	if rand.Float64() > 0.5 {
		typp = "market"
	}

	ob := worke.OrderBook{Symbol: "BTCUSDT", Price: (12000 * rand.Float64()), Amount: ((10 * rand.Float64()) + 0.001), Type: typp, Side: side}
	fmt.Printf("ob: %v\n", ob.Type)

	b, err := json.Marshal(ob)
	if err != nil {

	}

	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

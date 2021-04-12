package main

import (
	services "aapanavyapar-service-updater/startup-tasks"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/joho/godotenv/autoload"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	database := services.NewDataSource()

	fmt.Println(time.Now().UTC())
	var syn sync.WaitGroup

	noRequest := 1000000
	syn.Add(noRequest)

	for i := 0; i < noRequest; i++ {
		go func(uid string, sy *sync.WaitGroup) {
			database.Cash.Cash.XAdd(context.TODO(), &redis.XAddArgs{
				Stream:       os.Getenv("REDIS_STREAM_CART_NAME"),
				MaxLen:       0,
				MaxLenApprox: 0,
				ID:           "",
				Values:       []string{"uId", uid, "prodId", "606f30ec7bb389a9f1ff60c8", "operation", "+"},
			})
			syn.Done()
		}(strconv.Itoa(i), &syn)

		//go func(uid string, sy *sync.WaitGroup) {
		//	database.Cash.Cash.XAdd(context.TODO(), &redis.XAddArgs{
		//		Stream:       os.Getenv("REDIS_STREAM_FAV_NAME"),
		//		MaxLen:       0,
		//		MaxLenApprox: 0,
		//		ID:           "",
		//		Values:       []string{"uId", uid, "prodId", "606f30ec7bb389a9f1ff60c8", "operation", "+"},
		//	})
		//	syn.Done()
		//}(strconv.Itoa(i), &syn)

	}
	syn.Wait()
	fmt.Println(time.Now().UTC())

}

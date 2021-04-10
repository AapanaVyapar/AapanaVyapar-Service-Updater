package main

import (
	"aapanavyapar-service-viewprovider/configurations/mongodb"
	services "aapanavyapar-service-viewprovider/startup-tasks"
	"aapanavyapar-service-viewprovider/structs"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/joho/godotenv/autoload"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func SyncWithBasicCategories(dataBase *services.DataSources, group *sync.WaitGroup) {
	defaultDataCollection := mongodb.OpenDefaultDataCollection(dataBase.Data.Data)

	defer group.Done()

	defaultStream, err := defaultDataCollection.Watch(context.TODO(), mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		panic(err)
	}
	defer defaultStream.Close(context.TODO())

	for defaultStream.Next(context.TODO()) {
		var data structs.BasicCategoryStreamDecoding
		if err = defaultStream.Decode(&data); err != nil {
			panic(err)
		}
		fmt.Printf("Basic Category Stream : %v\n", data)

		if data.OperationType == "insert" || data.OperationType == "update" || data.OperationType == "replace" {
			fmt.Println(data.FullDocument)
			dataBase.Cash.Cash.HSet(context.TODO(), "categories", data.FullDocument.Category, data.FullDocument.Marshal())

		} else if data.OperationType == "delete" {
			dataBase.Cash.Cash.HDel(context.TODO(), "categories", data.DocumentKey.Id)

		}
		//var data bson.M
		//if err = defaultStream.Decode(&data); err != nil {
		//	panic(err)
		//}
		//fmt.Printf("%v\n", data)
		//
		//operationType := data["operationType"].(string)
		//
		//if operationType == "insert" || operationType == "update" || operationType == "replace" {
		//	fullDocument := data["fullDocument"].(bson.M)
		//	category := fullDocument["_id"].(string)
		//	subCategory := fullDocument["sub_categories"].(bson.A)
		//
		//	array := make([]string, len(subCategory))
		//	for i, ele := range subCategory {
		//		array[i] = ele.(string)
		//	}
		//
		//	categoriesData := structs.BasicCategoriesData{
		//		Category:      category,
		//		SubCategories: array,
		//	}
		//
		//	dataBase.Redis.HSet(context.TODO(), "categories", category, categoriesData.Marshal())
		//
		//} else if operationType == "delete" {
		//	documentKey := data["documentKey"].(bson.M)
		//	id := documentKey["_id"].(string)
		//
		//	dataBase.Redis.HDel(context.TODO(), "categories", id)
		//
		//}
	}
}

//
//func SyncWithShops(dataBase structs.DataBase, group *sync.WaitGroup) {
//	shopDataCollection := mongodb.OpenShopDataCollection(dataBase.Mongo)
//
//	defer group.Done()
//
//	shopStream, err := shopDataCollection.Watch(context.TODO(), mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
//	if err != nil {
//		panic(err)
//	}
//	defer shopStream.Close(context.TODO())
//
//	for shopStream.Next(context.TODO()) {
//		var data structs.ShopStreamDecoding
//		if err = shopStream.Decode(&data); err != nil {
//			panic(err)
//		}
//
//		//fmt.Printf("Shop Data Struct Decode : %v\n", data)
//		var dataTest bson.M
//		if err = shopStream.Decode(&dataTest); err != nil {
//			panic(err)
//		}
//		//fmt.Printf("Shop Data Bson Decode : %v\n", dataTest)
//
//		if data.OperationType == "insert" {
//			fmt.Println(data.FullDocument.Address)
//			fmt.Println(data.FullDocument.Location)
//			fmt.Println(data.FullDocument.Category)
//			fmt.Println(data.FullDocument.OperationalHours)
//			fmt.Println(data.FullDocument.Ratings)
//
//			dataBase.Redis.HSet(context.TODO(), "shops", data.FullDocument.ShopId.String(), data.FullDocument.Marshal())
//
//		} else if data.OperationType == "update" || data.OperationType == "replace"  {
//			fmt.Println(data.UpdateDescription.UpdatedFields.Address)
//			fmt.Println(data.UpdateDescription.UpdatedFields.Location)
//			fmt.Println(data.UpdateDescription.UpdatedFields.Category)
//			fmt.Println(data.UpdateDescription.UpdatedFields.OperationalHours)
//			fmt.Println(data.UpdateDescription.UpdatedFields.Ratings)
//
//
//		} else if data.OperationType == "delete" {
//			dataBase.Redis.HDel(context.TODO(), "shops", data.DocumentKey.Id.String())
//
//		}
//	}
//}

func SyncWithAddToFavorite(ctx context.Context, dataBase *services.DataSources, checkBackLog bool, group *sync.WaitGroup) {
	lastId := "0"
	defer group.Done()

	for {
		myKeyId := ">" //For Undelivered Ids So that Each Consumer Get Unique Id.
		if checkBackLog {
			myKeyId = lastId
		}

		readGroup := dataBase.Cash.Cash.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    os.Getenv("REDIS_STREAM_FAV_GROUP"),
			Consumer: os.Getenv("REDIS_WORKER_NAME"),
			Streams:  []string{os.Getenv("REDIS_STREAM_FAV_NAME"), myKeyId},
			Count:    10,  // No Of Data To Retrieve
			Block:    200, //TimeOut
			NoAck:    false,
		})

		if readGroup.Err() != nil {
			if strings.Contains(readGroup.Err().Error(), "timeout") {
				fmt.Println("TimeOUT")
				continue
			}
			panic(readGroup.Err())
		}

		data, err := readGroup.Result()
		if err != nil {
			panic(err)
		}

		fmt.Println("Val : ", readGroup.Val())
		fmt.Println(readGroup.Result())
		fmt.Println("Args : ", readGroup.Args())

		if len(data[0].Messages) == 0 {
			checkBackLog = false
			fmt.Println("Started Checking For New Messages ..!!")
			continue
		}

		fmt.Println(data[0].Messages)

		var val redis.XMessage
		fmt.Println("\n\n\n NEW : ", lastId)
		for _, val = range data[0].Messages {
			err = dataBase.PerformAddToFavorite(ctx, &val)
			if err != nil {
				fmt.Println("Context Error Please Check For Data Base Connectivity, Network Error Or Any Other Dependency Problem")
				checkBackLog = true
				val.ID = "0"
				break
			}
		}
		lastId = val.ID
	}
}

func main() {

	dataBase := services.NewDataSource()
	dataContext, cancel := context.WithDeadline(context.TODO(), time.Now().Add(time.Minute*5))
	dataBase.InitStartup(dataContext)
	cancel()

	checkBackLog, err := strconv.ParseBool(os.Getenv("REDIS_STREAM_CHECK_BACKLOG"))
	if err != nil {
		panic(err)
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)

	go SyncWithAddToFavorite(context.TODO(), dataBase, checkBackLog, &waitGroup)
	//go SyncWithBasicCategories(dataBase, &waitGroup)
	//go SyncWithShops(dataBase, &waitGroup)

	waitGroup.Wait()

}

/*
	Startup Service Tasks :
		1. Load Required Data To Cash.
		2. Create Stream And Make Group.
*/

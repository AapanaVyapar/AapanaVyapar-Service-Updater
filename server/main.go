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
	"os/signal"
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

	startFav = time.Now().UTC()

	for {
		myKeyId := ">" //For Undelivered Ids So that Each Consumer Get Unique Id.
		if checkBackLog {
			myKeyId = lastId

		}

		readGroup := dataBase.Cash.ReadFromFavStream(ctx, 10, 200, myKeyId)
		if readGroup.Err() != nil {
			if strings.Contains(readGroup.Err().Error(), "timeout") {
				fmt.Println("TimeOUT")
				continue

			}
			if readGroup.Err() == redis.Nil {
				fmt.Println("CART : No Data Available")
				continue

			}
			panic(readGroup.Err())

		}

		data, err := readGroup.Result()
		if err != nil {
			panic(err)

		}

		if len(data[0].Messages) == 0 {
			checkBackLog = false
			fmt.Println("Started Checking For New Messages ..!!")
			continue

		}

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

func SyncWithAddToCart(ctx context.Context, dataBase *services.DataSources, checkBackLog bool, group *sync.WaitGroup) {
	lastId := "0"
	defer group.Done()
	startCart = time.Now().UTC()

	for {
		myKeyId := ">" //For Undelivered Ids So that Each Consumer Get Unique Id.
		if checkBackLog {
			myKeyId = lastId

		}

		readGroup := dataBase.Cash.ReadFromCartStream(ctx, 10, 2000, myKeyId)
		if readGroup.Err() != nil {
			if strings.Contains(readGroup.Err().Error(), "timeout") {
				fmt.Println("CART : TimeOUT")
				continue

			}

			if readGroup.Err() == redis.Nil {
				fmt.Println("CART : No Data Available")
				continue

			}

			fmt.Println(readGroup)
			panic(readGroup.Err())
		}

		data, err := readGroup.Result()
		if err != nil {
			panic(err)

		}

		if len(data[0].Messages) == 0 {
			checkBackLog = false
			fmt.Println("Started Checking For New Messages ..!!")
			continue

		}

		var val redis.XMessage
		fmt.Println("\n\n\n NEW : ", lastId)
		for _, val = range data[0].Messages {
			err = dataBase.PerformAddToCart(ctx, &val)
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

var startCart = time.Now()
var startFav = time.Now()

func main() {

	dataBaseInit := services.NewDataSource()
	dataContext, cancel := context.WithDeadline(context.TODO(), time.Now().Add(time.Minute*5))
	dataBaseInit.InitStartup(dataContext)
	_ = dataBaseInit.Data.Data.Disconnect(dataContext)
	cancel()

	checkBackLog, err := strconv.ParseBool(os.Getenv("REDIS_STREAM_CHECK_BACKLOG"))
	if err != nil {
		panic(err)
	}

	dataBaseFav := services.NewDataSource()
	dataBaseCart := services.NewDataSource()

	defer dataBaseFav.Data.Data.Disconnect(context.TODO())
	defer dataBaseCart.Data.Data.Disconnect(context.TODO())

	err = dataBaseFav.InitFavStream(context.TODO())
	if err != nil {
		panic(err)
	}

	err = dataBaseCart.InitCartStream(context.TODO())
	if err != nil {
		panic(err)
	}

	var waitGroup sync.WaitGroup

	//Different Connections
	// FAV :   2021-04-10 15:08:28.211232546 +0000 UTC
	// Start Cart :  2021-04-10 15:06:30.480562923 +0000 UTC
	// Diff = 2

	//Single Connection
	//CART :   2021-04-10 15:15:36.040009232 +0000 UTC
	//Start Cart :  2021-04-10 15:13:39.261079629 +0000 UTC
	// Diff = 2

	//Single Connection On Shard
	//CART :   2021-04-10 15:24:24.531923312 +0000 UTC
	//Start Cart :  2021-04-10 15:20:38.035263452 +0000 UTC
	// Diff = 4

	//Different Connection On Shard
	//CART :   2021-04-10 15:34:45.318704813 +0000 UTC
	//Start Cart :  2021-04-10 15:30:10.169448625 +0000 UTC
	// Diff = 4

	//Different Connection On Shard
	//CART :   2021-04-10 17:51:48.696856556 +0000 UTC
	//Start Cart :  2021-04-10 17:46:10.60028291 +0000 UTC

	waitGroup.Add(2)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			dataBaseFav.Data.Data.Disconnect(context.TODO())
			dataBaseCart.Data.Data.Disconnect(context.TODO())
			fmt.Println("\n\n\nStart Cart : ", startFav)
			fmt.Println("\n\n\nStart Fav : ", startCart)
			break
		}
	}()

	go SyncWithAddToCart(context.TODO(), dataBaseCart, checkBackLog, &waitGroup)
	go SyncWithAddToFavorite(context.TODO(), dataBaseFav, checkBackLog, &waitGroup)
	//go SyncWithBasicCategories(dataBase, &waitGroup)
	//go SyncWithShops(dataBase, &waitGroup)

	waitGroup.Wait()

}

/*
	Startup Service Tasks :
		1. Load Required Data To Cash.
		2. Create Stream And Make Group.
*/

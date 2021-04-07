package main

import (
	"aapanavyapar-service-viewprovider/configurations/mongodb"
	"aapanavyapar-service-viewprovider/configurations/redis"
	"aapanavyapar-service-viewprovider/structures"
	"context"
	"fmt"
	_ "github.com/joho/godotenv/autoload"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

func SyncWithBasicCategories(dataBase structures.DataBase, group *sync.WaitGroup) {
	defaultDataCollection := mongodb.OpenDefaultDataCollection(dataBase.Mongo)

	defer group.Done()

	defaultStream, err := defaultDataCollection.Watch(context.TODO(), mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		panic(err)
	}
	defer defaultStream.Close(context.TODO())

	for defaultStream.Next(context.TODO()) {
		var data bson.M
		if err = defaultStream.Decode(&data); err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", data)

		operationType := data["operationType"].(string)

		if operationType == "insert" || operationType == "update" || operationType == "replace" {
			fullDocument := data["fullDocument"].(bson.M)
			category := fullDocument["_id"].(string)
			subCategory := fullDocument["sub_categories"].(bson.A)

			array := make([]string, len(subCategory))
			for i, ele := range subCategory {
				array[i] = ele.(string)
			}

			categoriesData := structures.BasicCategoriesData{
				Category:      category,
				SubCategories: array,
			}

			dataBase.Redis.HSet(context.TODO(), "categories", category, categoriesData.Marshal())

		} else if operationType == "delete" {
			documentKey := data["documentKey"].(bson.M)
			id := documentKey["_id"].(string)

			dataBase.Redis.HDel(context.TODO(), "categories", id)

		}
	}
}

func main() {

	dataBase := structures.DataBase{
		Mongo: mongodb.InitMongo(),
		Redis: redis.InitRedis(),
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)

	go SyncWithBasicCategories(dataBase, &waitGroup)

	waitGroup.Wait()

}

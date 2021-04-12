package services

import (
	"aapanavyapar-service-updater/configurations/mongodb"

	"aapanavyapar-service-updater/structs"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

func SyncWithBasicCategories(dataBase *DataSources, group *sync.WaitGroup) {
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

func SyncWithShops(dataBase *DataSources, group *sync.WaitGroup) {
	shopDataCollection := mongodb.OpenShopDataCollection(dataBase.Data.Data)

	defer group.Done()

	shopStream, err := shopDataCollection.Watch(context.TODO(), mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		panic(err)
	}
	defer shopStream.Close(context.TODO())

	for shopStream.Next(context.TODO()) {
		var data structs.ShopStreamDecoding
		if err = shopStream.Decode(&data); err != nil {
			panic(err)
		}

		//fmt.Printf("Shop Data Struct Decode : %v\n", data)
		var dataTest bson.M
		if err = shopStream.Decode(&dataTest); err != nil {
			panic(err)
		}
		//fmt.Printf("Shop Data Bson Decode : %v\n", dataTest)

		if data.OperationType == "insert" {
			fmt.Println(data.FullDocument.Address)
			fmt.Println(data.FullDocument.Location)
			fmt.Println(data.FullDocument.Category)
			fmt.Println(data.FullDocument.OperationalHours)
			fmt.Println(data.FullDocument.Ratings)

			err = dataBase.Cash.AddShopDataToCash(context.TODO(), data.FullDocument.ShopId.Hex(), data.FullDocument.Marshal())
			array := structs.CashStructureProductArray{
				Products: []string{},
			}

			err = dataBase.Cash.AddShopProductMapDataToCash(context.TODO(), data.FullDocument.ShopId.Hex(), array.Marshal())
			if err != nil {
				//return err
			}

		} else if data.OperationType == "update" || data.OperationType == "replace" {
			fmt.Println(data.UpdateDescription.UpdatedFields.Address)
			fmt.Println(data.UpdateDescription.UpdatedFields.Location)
			fmt.Println(data.UpdateDescription.UpdatedFields.Category)
			fmt.Println(data.UpdateDescription.UpdatedFields.OperationalHours)
			fmt.Println(data.UpdateDescription.UpdatedFields.Ratings)

		} else if data.OperationType == "delete" {
			dataBase.Cash.Cash.HDel(context.TODO(), "shops", data.DocumentKey.Id.String())

		}
	}
}

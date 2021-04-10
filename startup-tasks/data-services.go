package services

import (
	"aapanavyapar-service-viewprovider/configurations/mongodb"
	"aapanavyapar-service-viewprovider/structs"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

type MongoDataBase struct {
	Data  *mongo.Client
	mutex sync.RWMutex
}

func NewMongoClient() *MongoDataBase {
	return &MongoDataBase{Data: mongodb.InitMongo()}
}

func (dataBase *MongoDataBase) GetAllBasicCategories(context context.Context, sendData func(data structs.BasicCategoriesData) error) error {

	defaultData := mongodb.OpenDefaultDataCollection(dataBase.Data)

	cursor, err := defaultData.Find(context, bson.D{})

	if err != nil {
		return err
	}
	defer cursor.Close(context)

	for cursor.Next(context) {
		result := structs.BasicCategoriesData{}
		err = cursor.Decode(&result)

		fmt.Println(result.Category)
		fmt.Println(result.SubCategories)

		if err != nil {
			return err
		}

		if err = sendData(result); err != nil {
			return err
		}

	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return nil

}

func (dataBase *MongoDataBase) GetAllShopsFromShopData(context context.Context, sendData func(data structs.ShopData) error) error {

	shopData := mongodb.OpenShopDataCollection(dataBase.Data)

	filter := bson.D{}
	cursor, err := shopData.Find(context, filter)

	if err != nil {
		return err
	}
	defer cursor.Close(context)

	for cursor.Next(context) {
		result := structs.ShopData{}
		err = cursor.Decode(&result)

		if err != nil {
			return err
		}

		if err = sendData(result); err != nil {
			return err
		}

	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return nil

}

func (dataBase *MongoDataBase) GetAllProductsFromProductData(context context.Context, sendData func(data structs.ProductData) error) error {

	productData := mongodb.OpenProductDataCollection(dataBase.Data)

	filter := bson.D{}
	cursor, err := productData.Find(context, filter)

	if err != nil {
		return err
	}
	defer cursor.Close(context)

	for cursor.Next(context) {
		result := structs.ProductData{}
		err = cursor.Decode(&result)

		if err != nil {
			return err
		}

		if err = sendData(result); err != nil {
			return err
		}

	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return nil

}
func (dataBase *MongoDataBase) IsExistProductExist(context context.Context, key string, value interface{}) error {
	productData := mongodb.OpenProductDataCollection(dataBase.Data)

	filter := bson.D{{key, value}}
	singleCursor := productData.FindOne(context, filter)

	if singleCursor.Err() != nil {
		return singleCursor.Err()
	}

	return nil

}

func (dataBase *MongoDataBase) AddToCartUserData(context context.Context, userId string, productId primitive.ObjectID) error {

	//Checking In Cash For Existence Of Product So No Need To Check In DataBase And If Some Inconsistency Occurs
	//Then What When User Add Other Items In Cart It Get Automatically Deleted And In Front End We Make That Product Id Invisible If Product Not Exist
	//
	//if !dataBase.IsExistProductExist(context, "_id", productId) {
	//	return fmt.Errorf("product does not exist")
	//}
	//

	userData := mongodb.OpenUserDataCollection(dataBase.Data)

	// All Database Operation on single document are atomic in mongodb
	//dataBase.mutex.Lock()
	//defer dataBase.mutex.Unlock()

	result := userData.FindOne(context, bson.M{"_id": userId, "cart.products": productId})

	// Error will be thrown if favorites is null or product is not in favorites in both cases we have to just add product
	if result.Err() != nil {
		fmt.Println("CART : ", result.Err())
		res, err := userData.UpdateOne(context,
			bson.M{
				"_id": userId,
			},
			bson.D{
				{"$push",
					bson.M{
						"cart.products": bson.M{
							"$each":  bson.A{productId},
							"$slice": -15,
						},
					},
				},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return err
		}

		if res.ModifiedCount > 0 || res.MatchedCount > 0 || res.UpsertedCount > 0 {
			return nil
		}

		return fmt.Errorf("unable to add to cart")
	}

	return fmt.Errorf("alredy exist in cart")
}

func (dataBase *MongoDataBase) DelFromCartUserData(context context.Context, userId string, productId primitive.ObjectID) error {

	userData := mongodb.OpenUserDataCollection(dataBase.Data)

	// All Database Operation on single document are atomic in mongodb
	//dataBase.mutex.Lock()
	//defer dataBase.mutex.Unlock()

	result, err := userData.UpdateOne(context,
		bson.M{
			"_id": userId,
		},
		bson.M{
			"$pull": bson.M{
				"cart.products": productId,
			},
		},
	)

	if err != nil {
		return err
	}

	if result.ModifiedCount > 0 || result.MatchedCount > 0 {
		return nil
	}

	return fmt.Errorf("unable to delete from cart")
}

func (dataBase *MongoDataBase) AddToFavoritesUserData(context context.Context, userId string, productId primitive.ObjectID) error {

	//if err := dataBase.IsExistProductExist(context, "_id", productId); err != nil {
	//	return err
	//}

	userData := mongodb.OpenUserDataCollection(dataBase.Data)

	// All Database Operation on single document are atomic in mongodb
	//dataBase.mutex.Lock()
	//defer dataBase.mutex.Unlock()

	result := userData.FindOne(context, bson.M{"_id": userId, "favorites.products": productId})

	// Error will be thrown if favorites is null or product is not in favorites in both cases we have to just add product
	if result.Err() != nil {
		fmt.Println(result.Err())
		fmt.Println("result : ", result.Err())

		res, err := userData.UpdateOne(context,
			bson.M{
				"_id": userId,
			},
			bson.D{
				{"$push",
					bson.M{
						"favorites.products": bson.M{
							"$each":  bson.A{productId},
							"$slice": -20,
						},
					},
				},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return err
		}

		if res.ModifiedCount > 0 || res.MatchedCount > 0 || res.UpsertedCount > 0 {
			return nil
		}

		return fmt.Errorf("unable to add to faviroute")
	}

	return fmt.Errorf("alredy exist in faviroute")
}

func (dataBase *MongoDataBase) DelFromFavoritesUserData(context context.Context, userId string, productId primitive.ObjectID) error {

	userData := mongodb.OpenUserDataCollection(dataBase.Data)

	// All Database Operation on single document are atomic in mongodb
	//dataBase.mutex.Lock()
	//defer dataBase.mutex.Unlock()

	result, err := userData.UpdateOne(context,
		bson.M{
			"_id": userId,
		},
		bson.M{
			"$pull": bson.M{
				"favorites.products": productId,
			},
		},
	)

	if err != nil {
		return err
	}

	if result.ModifiedCount > 0 || result.MatchedCount > 0 {
		return nil
	}

	return fmt.Errorf("unable to delete from faviroute")
}

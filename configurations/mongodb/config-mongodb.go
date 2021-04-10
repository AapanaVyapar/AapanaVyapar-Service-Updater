package mongodb

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"time"
)

func InitMongo() *mongo.Client {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	credential := options.Credential{
		Username: os.Getenv("MONGODB_USER"),
		Password: os.Getenv("MONGODB_PASSWORD"),
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGODB_URI")).SetAuth(credential).SetMaxPoolSize(100))
	if err != nil {
		panic(err)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx, nil)

	if err != nil {
		panic(err)
	}

	return client

}

func OpenDefaultDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("defaultData")
	return userData
}

func OpenUserDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("userData")
	return userData
}

func OpenOrderDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("orderData")
	return userData
}

func OpenShopDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("shopData")
	return userData
}

func OpenProductDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("productData")
	return userData
}

func OpenAnalyticalDataCollection(client *mongo.Client) *mongo.Collection {
	database := client.Database("db_aapanavypar")
	userData := database.Collection("analyticalData")
	return userData
}

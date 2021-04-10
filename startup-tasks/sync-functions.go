package services

import (
	"aapanavyapar-service-viewprovider/helpers"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"time"
)

/*
	In Below Function We Only Care For Context Error And This Is Because If Database Failed Then Message Should Not Get Acknowledge
	Else If There Is Some User Not Exist Or Other Operation Related Error We No Need To Care For That Because That Is Related To User Input Process.
	This Will Allow Us To Make Stream Empty From Bad Messages.
*/
func (dataSource *DataSources) PerformAddToFavorite(ctx context.Context, val *redis.XMessage) error {

	uId := val.Values["uId"].(string)
	prodId := val.Values["prodId"].(string)
	operation := val.Values["operation"].(string)

	fmt.Println(uId)
	fmt.Println(prodId)
	fmt.Println(operation)

	productId, err := primitive.ObjectIDFromHex(prodId)
	if err != nil {
		dataSource.Cash.Cash.XAck(ctx, os.Getenv("REDIS_STREAM_FAV_NAME"), os.Getenv("REDIS_STREAM_FAV_GROUP"), val.ID)
		dataSource.Cash.Cash.XDel(ctx, os.Getenv("REDIS_STREAM_FAV_NAME"), val.ID)
		return nil
	}

	dataContext, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*4))

	if operation == "+" {
		err := dataSource.Data.AddToFavoritesUserData(dataContext, uId, productId)
		fmt.Println(err)
		if err != nil && helpers.ContextError(dataContext) != nil {
			cancel()
			return err
		}

	} else {
		err := dataSource.Data.DelFromFavoritesUserData(dataContext, uId, productId)
		if err != nil && helpers.ContextError(dataContext) != nil {
			fmt.Println(err)
			cancel()
			return err
		}

	}

	fmt.Println("Done Acknowledge")
	dataSource.Cash.Cash.XAck(ctx, os.Getenv("REDIS_STREAM_FAV_NAME"), os.Getenv("REDIS_STREAM_FAV_GROUP"), val.ID)
	dataSource.Cash.Cash.XDel(ctx, os.Getenv("REDIS_STREAM_FAV_NAME"), val.ID)
	cancel()
	return nil
}

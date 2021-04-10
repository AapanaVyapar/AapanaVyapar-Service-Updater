package services

import (
	"aapanavyapar-service-viewprovider/configurations/redisdb"
	"context"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RedisDataBase struct {
	Cash *redis.Client
}

func NewRedisClient() *RedisDataBase {
	return &RedisDataBase{Cash: redisdb.InitRedis()}
}

func (dataBase *RedisDataBase) AddShopDataToCash(ctx context.Context, shopId string, data interface{}) error {

	err := dataBase.Cash.HSet(ctx, "shops", shopId, data).Err()
	if err != nil {
		return status.Errorf(codes.Internal, "unable to add data to hash of Cash  : %w", err)
	}
	return nil

}

func (dataBase *RedisDataBase) AddShopProductMapDataToCash(ctx context.Context, shopId string, data interface{}) error {

	err := dataBase.Cash.HSet(ctx, "shopProductMap", shopId, data).Err()
	if err != nil {
		return status.Errorf(codes.Internal, "unable to add data to hash of Cash  : %w", err)
	}
	return nil

}

func (dataBase *RedisDataBase) AddProductDataToCash(ctx context.Context, productId string, data interface{}) error {

	err := dataBase.Cash.HSet(ctx, "products", productId, data).Err()
	if err != nil {
		return status.Errorf(codes.Internal, "unable to add data to hash of Cash  : %w", err)
	}
	return nil

}

func (dataBase *RedisDataBase) GetShopProductMapDataFromCash(ctx context.Context, productId string) (string, error) {

	val, err := dataBase.Cash.HGet(ctx, "shopProductMap", productId).Result()
	switch {
	case err == redis.Nil:
		return "", status.Errorf(codes.NotFound, "Value Not Exist %v", err)
	case err != nil:
		return "", status.Errorf(codes.Internal, "Unable To Fetch Value %v", err)
	case val == "":
		return "", status.Errorf(codes.Unknown, "Empty Value %v", err)
	}
	return val, nil

}

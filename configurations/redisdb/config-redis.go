package redisdb

import (
	"github.com/go-redis/redis/v8"
	"os"
	"strconv"
)

func InitRedis() *redis.Client {
	dbName, _ := strconv.Atoi(os.Getenv("REDIS_DB"))

	redisDb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDRESS"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       dbName,
	})
	return redisDb
}

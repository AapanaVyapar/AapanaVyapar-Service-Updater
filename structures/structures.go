package structures

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/mongo"
)

type DataBase struct {
	Mongo *mongo.Client
	Redis *redis.Client
}

type BasicCategoriesData struct {
	Category      string   `bson:"_id" json:"_id" validate:"required"`
	SubCategories []string `bson:"sub_categories,omitempty" json:"sub_categories" validate:"required"`
}

func (m *BasicCategoriesData) Marshal() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
	}
	return data
}

func UnmarshalSubCategories(data []byte, m *BasicCategoriesData) {
	err := json.Unmarshal(data, &m)
	if err != nil {
		fmt.Println(err)
	}
}

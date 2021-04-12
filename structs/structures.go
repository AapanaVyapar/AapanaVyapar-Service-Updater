package structs

import (
	"aapanavyapar-service-updater/pb"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type UserData struct {
	UserId    string                  `bson:"_id" json:"_id" validate:"required"`
	UserName  string                  `bson:"user_name" json:"user_name" validate:"required"`
	Address   *Address                `bson:"address,omitempty" json:"address"`
	Cart      *ProductIdsForCart      `bson:"cart,omitempty" json:"cart"`
	Favorites *ProductIdsForFavAndOrd `bson:"favorites,omitempty" json:"favorites"`
	Orders    *ProductIdsForFavAndOrd `bson:"orders,omitempty" json:"orders"`
}

type ProductIdsForCart struct {
	Products []primitive.ObjectID `bson:"products,omitempty" json:"products"`
}

type ProductIdsForFavAndOrd struct {
	Products []primitive.ObjectID `bson:"products,omitempty" json:"products"`
}

type OrderData struct {
	OrderId           primitive.ObjectID `bson:"_id,omitempty" json:"_id"`
	UserId            string             `bson:"user_id" json:"user_id" validate:"required"`
	Status            pb.Status          `bson:"status" json:"status" validate:"required"`
	ProductId         primitive.ObjectID `bson:"product_id" json:"product_id" validate:"required"`
	DeliveryTimeStamp time.Time          `bson:"delivery_time_stamp" json:"delivery_time_stamp" validate:"required"`
	OrderTimeStamp    time.Time          `bson:"order_time_stamp" json:"order_time_stamp" validate:"required"`
	Address           *Address           `bson:"address,omitempty" json:"address" validate:"required"`
	Price             float32            `bson:"price" json:"price" validate:"required"`
	DeliveryCost      float32            `bson:"delivery_cost" json:"delivery_cost" validate:"required"`
	Quantity          uint32             `bson:"quantity" json:"quantity" validate:"required"`
}

type Address struct {
	FullName      string `bson:"full_name" json:"name" validate:"required,min=2,max=100"`
	HouseDetails  string `bson:"house_details" json:"house_details" validate:"required,max=100"`
	StreetDetails string `bson:"street_details" json:"street_details" validate:"required,max=100"`
	LandMark      string `bson:"land_mark" json:"land_mark" validate:"required,max=100"`
	PinCode       string `bson:"code" json:"code" validate:"required,max=6"`
	City          string `bson:"city" json:"city" validate:"required,max=90"`
	State         string `bson:"state" json:"state" validate:"required,max=90"`
	Country       string `bson:"country" json:"country" validate:"required,max=90"`
	PhoneNo       string `bson:"phone_no" json:"phone_no" validate:"required,max=10"`
}

type Location struct {
	Longitude string `bson:"longitude" json:"longitude" validate:"required,longitude"`
	Latitude  string `bson:"latitude" json:"latitude" validate:"required,latitude"`
}

type OperationalHours struct {
	Sunday    [2]string `json:"sunday" validate:"required"`
	Monday    [2]string `json:"monday" validate:"required"`
	Tuesday   [2]string `json:"tuesday" validate:"required"`
	Wednesday [2]string `json:"wednesday" validate:"required"`
	Thursday  [2]string `json:"thursday" validate:"required"`
	Friday    [2]string `json:"friday" validate:"required"`
	Saturday  [2]string `json:"saturday" validate:"required"`
}

type Rating struct {
	UserId    string     `bson:"user_id" json:"user_id" validate:"required"`
	UserName  string     `bson:"user_name" json:"user_name" validate:"required"`
	Comment   string     `bson:"comment" json:"comment" validate:"required,max=100"`
	Rating    pb.Ratings `bson:"rating" json:"rating" validate:"required"`
	Timestamp time.Time  `bson:"timestamp" json:"timestamp" validate:"required"`
}

type ShopData struct {
	ShopId              string            `bson:"_id" json:"_id" validate:"required"`
	ShopName            string            `bson:"shop_name" json:"shop_name" validate:"required,max=50"`
	ShopKeeperName      string            `bson:"shop_keeper_name" json:"shop_keeper_name" validate:"required,min=2,max=100"`
	Images              []string          `bson:"images" json:"images" validate:"required"`
	PrimaryImage        string            `bson:"primary_image" json:"primary_images" validate:"required,url"`
	Address             *Address          `bson:"address" json:"address" validate:"required"`
	Location            *Location         `bson:"location" json:"location" validate:"required"`
	SectorNo            int32             `bson:"sector_no" json:"sector_no"`
	Category            []pb.Category     `bson:"category" json:"category" validate:"required"`
	BusinessInformation string            `bson:"business_information" json:"business_information" validate:"required,max=500"`
	OperationalHours    *OperationalHours `bson:"operational_hours" json:"operational_hours" validate:"required"`
	Ratings             *[]Rating         `bson:"ratings,omitempty" json:"ratings"`
	Timestamp           time.Time         `bson:"timestamp" json:"timestamp" validate:"required"`
}

type ProductData struct {
	ProductId        primitive.ObjectID `bson:"_id,omitempty" json:"_id"`
	ShopId           string             `bson:"shop_id" json:"shop_id" validate:"required"`
	ShopName         string             `bson:"shop_name" json:"shop_name"`
	Title            string             `bson:"title" json:"title" validate:"required"`
	ShortDescription string             `bson:"short_description" json:"short_description" validate:"required"`
	Description      string             `bson:"description" json:"description" validate:"required"`
	ShippingInfo     string             `bson:"shipping_info" json:"shipping_info" validate:"required"`
	Stock            uint32             `bson:"stock" json:"stock" validate:"max=4294967290"`
	Likes            uint64             `bson:"likes" json:"likes" validate:"max=4294967290"`
	Price            float32            `bson:"price" json:"price" validate:"required"`
	Offer            uint32             `bson:"offer" json:"offer" validate:"required,max=100"`
	Images           []string           `bson:"images" json:"images" validate:"required"`
	Category         []pb.Category      `bson:"category" json:"category" validate:"required"`
	Timestamp        time.Time          `bson:"timestamp" json:"timestamp" validate:"required"`
}

type AnalyticalClickData struct {
	ProductId primitive.ObjectID `bson:"product_id" json:"product_id" validate:"required"`
	Timestamp time.Time          `bson:"timestamp" json:"timestamp" validate:"required"`
	Category  []pb.Category      `bson:"category" json:"category" validate:"required"`
}

type MostVisited struct {
	Product []AnalyticalClickData `bson:"product,omitempty" json:"product" validate:"required"`
}

type AnalyticalData struct {
	UserId      string       `bson:"_id" json:"_id" validate:"required"`
	MostVisited *MostVisited `bson:"most_visited,omitempty" json:"most_visited" validate:"required"`
}

type BasicCategoriesData struct {
	Category      string   `bson:"_id" json:"_id" validate:"required"`
	SubCategories []string `bson:"sub_categories,omitempty" json:"sub_categories" validate:"required"`
}

type CashStructureProductArray struct {
	Products []string `json:"products"`
}

type ShopStreamDocumentKey struct {
	Id primitive.ObjectID `bson:"_id" json:"_id"`
}

type UpdateDescription struct {
	UpdatedFields ShopData `bson:"updatedFields" json:"updatedFields"`
}

type ShopStreamDecoding struct {
	OperationType     string                `bson:"operationType" json:"operationType"`
	FullDocument      ShopData              `bson:"fullDocument" json:"fullDocument"`
	DocumentKey       ShopStreamDocumentKey `bson:"documentKey" json:"documentKey"`
	UpdateDescription UpdateDescription     `bson:"updateDescription" json:"updateDescription"`
}

type BasicCategoriesDataStreamDocumentKey struct {
	Id string `bson:"_id" json:"_id"`
}

type BasicCategoryStreamDecoding struct {
	OperationType string                               `bson:"operationType" json:"operationType"`
	FullDocument  BasicCategoriesData                  `bson:"fullDocument" json:"fullDocument"`
	DocumentKey   BasicCategoriesDataStreamDocumentKey `bson:"documentKey" json:"documentKey"`
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

func (m *ProductData) Marshal() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
	}
	return data
}

func UnmarshalProductData(data []byte, m *ProductData) {
	err := json.Unmarshal(data, &m)
	if err != nil {
		fmt.Println(err)
	}
}

func (m *ShopData) Marshal() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
	}
	return data
}

func UnmarshalShopData(data []byte, m *ShopData) {
	err := json.Unmarshal(data, &m)
	if err != nil {
		fmt.Println(err)
	}
}

func (m *CashStructureProductArray) Marshal() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
	}
	return data
}

func UnmarshalCashStructureProductArray(data []byte, m *CashStructureProductArray) {
	err := json.Unmarshal(data, &m)
	if err != nil {
		fmt.Println(err)
	}
}

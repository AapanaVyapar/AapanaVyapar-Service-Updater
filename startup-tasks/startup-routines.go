package services

import (
	"aapanavyapar-service-updater/structs"
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataSources struct {
	Data *MongoDataBase
	Cash *RedisDataBase
}

func NewDataSource() *DataSources {
	return &DataSources{
		Data: NewMongoClient(),
		Cash: NewRedisClient(),
	}
}

func (dataSource *DataSources) InitStartup(context context.Context) {
	err := dataSource.LoadBasicCategoriesInCash(context)
	if err != nil {
		panic(err)
	}

	err = dataSource.LoadShopsInCash(context)
	if err != nil {
		panic(err)
	}

	err = dataSource.LoadProductsInCash(context)
	if err != nil {
		panic(err)
	}

}

func (dataSource *DataSources) LoadBasicCategoriesInCash(ctx context.Context) error {

	err := dataSource.Data.GetAllBasicCategories(ctx, func(data structs.BasicCategoriesData) error {
		err := dataSource.Cash.Cash.HSet(ctx, "categories", data.Category, data.Marshal()).Err()
		if err != nil {
			return status.Errorf(codes.Internal, "unable to add data to hash of Cash  : %w", err)
		}
		return nil

	})
	if err != nil {
		return err

	}
	return nil

}

func (dataSource *DataSources) LoadShopsInCash(ctx context.Context) error {

	err := dataSource.Data.GetAllShopsFromShopData(ctx, func(data structs.ShopData) error {

		err := dataSource.Cash.AddShopDataToCash(ctx, data.ShopId.Hex(), data.Marshal())
		if err != nil {
			return err
		}

		array := structs.CashStructureProductArray{
			Products: []string{},
		}

		err = dataSource.Cash.AddShopProductMapDataToCash(ctx, data.ShopId.Hex(), array.Marshal())
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		return err

	}
	return nil

}

func (dataSource *DataSources) LoadProductsInCash(ctx context.Context) error {

	err := dataSource.Data.GetAllProductsFromProductData(ctx, func(data structs.ProductData) error {

		err := dataSource.Cash.AddProductDataToCash(ctx, data.ProductId.Hex(), data.Marshal())
		if err != nil {
			return err
		}

		val, err := dataSource.Cash.GetShopProductMapDataFromCash(ctx, data.ShopId.Hex())
		if err != nil {
			return err
		}

		array := structs.CashStructureProductArray{}
		structs.UnmarshalCashStructureProductArray([]byte(val), &array)

		array.Products = append(array.Products, data.ProductId.Hex())

		err = dataSource.Cash.AddShopProductMapDataToCash(ctx, data.ShopId.Hex(), array.Marshal())
		if err != nil {
			return err
		}

		return nil

	})
	if err != nil {
		return err

	}
	return nil

}

package main

import (
	services "aapanavyapar-service-viewprovider/startup-tasks"
	"context"
	_ "github.com/joho/godotenv/autoload"
	"sync"
	"time"
)

func main() {

	dataBaseInit := services.NewDataSource()
	dataContext, cancel := context.WithDeadline(context.TODO(), time.Now().Add(time.Minute*10))
	dataBaseInit.InitStartup(dataContext)
	_ = dataBaseInit.Data.Data.Disconnect(dataContext)
	cancel()

	dataBase := services.NewDataSource()

	var waitGroup sync.WaitGroup

	waitGroup.Add(2)

	//c := make(chan os.Signal, 1)
	//signal.Notify(c, os.Interrupt)
	//go func() {
	//	for range c {
	//		fmt.Println("\n\n\nDo Something : ")
	//		break
	//	}
	//}()

	go services.SyncWithBasicCategories(dataBase, &waitGroup)
	go services.SyncWithShops(dataBase, &waitGroup)

	waitGroup.Wait()

}

/*
	Startup Service Tasks :
		1. Load Required Data To Cash.
		2. Create Stream And Make Group.
*/

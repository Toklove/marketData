package main

import (
	"context"
	"fmt"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
)

func SubscribeInit() {
	//3.1从数据库中获取交易分类

	var marketCategory []MarketCategory

	mysqlDb.Table("market_categories").Find(&marketCategory)

	for _, category := range marketCategory {

		market, topic := getCategory(category.Name)

		//create a new client
		c, err := polygonws.New(polygonws.Config{
			APIKey: PolygonApiKey,
			Feed:   polygonws.RealTime,
			Market: market,
			Log:    logger,
		})

		if err != nil {
			fmt.Println("连接失败poly")
			logger.Fatal(err)
		}

		// connect to the server
		if err := c.Connect(); err != nil {
			fmt.Println("连接失败poly2")
			logger.Println(err)
			return
		}

		defer c.Close()

		var markets []Market

		mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

		if len(markets) == 0 {
			continue
		}

		tickers := make([]string, len(markets))

		//循环导出交易对
		for _, item := range markets {
			//判断交易对是否存在 如果不存在则创建
			names, _ := db.ListCollectionNames(context.Background(), nil)

			has := false

			for _, name := range names {
				if name == item.Symbol {
					has = true
					break
				}
			}

			if !has {
				err := db.CreateCollection(context.TODO(), item.Symbol)
				if err != nil {
					return
				}
			}

			tickers = append(tickers, item.Symbol)
		}

		if err := c.Subscribe(topic, tickers...); err != nil {
			logger.Fatal(err)
		}

		for {
			select {
			case err := <-c.Error():
				logger.Fatal(err)
			case out, more := <-c.Output():
				if !more {
					return
				}
				switch out.(type) {
				case models.CurrencyAgg:
					out := out.(models.CurrencyAgg)
					collectionName := out.Pair
					//保存到mongoDB中
					collection := db.Collection(collectionName)

					//根据Symbol和Timestamp判断是否存在
					var result marketData
					err := collection.FindOne(context.Background(), marketData{Symbol: out.Pair, Timestamp: out.EndTimestamp}).Decode(&result)
					if err != nil {
						logger.Info(err)
					}
					//如果存在则不保存
					if result.Symbol == "" {
						//转换成marketData
						var data = marketData{
							Open:      out.Open,
							High:      out.High,
							Low:       out.Low,
							Close:     out.Close,
							Symbol:    out.Pair,
							Timestamp: out.EndTimestamp,
							Volume:    out.Volume,
						}

						subscribe <- data

						_, err := collection.InsertOne(context.Background(), data)
						if err != nil {
							logger.Info(err)
						}
					}
				}
			}
		}

	}
}

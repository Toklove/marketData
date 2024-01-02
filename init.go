package main

import (
	"context"
	"fmt"
	restModels "github.com/polygon-io/client-go/rest/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"time"
)

func HistoryInit() {
	fmt.Println("开始初始化历史数据")

	//判断是否初始化 创建一个本地LOCK文件
	_, err := os.ReadFile("init.lock")

	if err == nil {
		fmt.Println("已经初始化")
		return
	}

	var marketCategory []MarketCategory

	mysqlDb.Table("market_categories").Find(&marketCategory)

	var doneArr []chan bool
	var arrLen = 0

	for _, category := range marketCategory {

		var markets []Market

		mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

		if len(markets) == 0 {
			continue
		}

		for _, item := range markets {
			arrLen++
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
					logger.Info(err)
				}

				//var lastTime =

				colllection := db.Collection(item.Symbol)
				//根据timestamp倒序排列
				var data MarketData
				colllection.FindOne(context.Background(), bson.D{}, options.FindOne().SetSort(bson.D{{"timestamp", -1}})).Decode(&data)

				lastTime := time.Now()

				if data.Timestamp != 0 {
					//获取最后一条数据的时间
					lastTime = time.Unix(data.Timestamp/1000, 0)
				} else {
					lastTime = time.Now().AddDate(0, 0, -7)
				}

				logger.Info("导出时间:" + lastTime.String())

				//获取七天行情
				limit := 50000
				order := restModels.Asc
				params := restModels.ListAggsParams{
					Ticker:     item.SymbolHistory,
					From:       restModels.Millis(lastTime),
					To:         restModels.Millis(time.Now()),
					Order:      &order,
					Limit:      &limit,
					Timespan:   restModels.Second,
					Multiplier: 1,
				}

				iter := restClient.AggsClient.ListAggs(context.TODO(), &params)
				logger.Info("开始导出" + item.Symbol)
				for iter.Next() {
					out := iter.Item()
					//保存到mongoDB中
					collection := db.Collection(item.Symbol)

					//根据Symbol和Timestamp判断是否存在
					var result MarketData

					//将timestamp转换为int64
					timestamp := time.Time(out.Timestamp).Unix() * 1000

					collection.FindOne(context.TODO(), MarketData{Symbol: item.Symbol, Timestamp: timestamp}).Decode(&result)
					//如果存在则不保存
					if result.Symbol == "" {
						//转换成marketData
						var data = MarketData{
							Open:      out.Open,
							High:      out.High,
							Low:       out.Low,
							Close:     out.Close,
							Symbol:    item.Symbol,
							Timestamp: timestamp,
							Volume:    out.Volume,
						}
						collection.InsertOne(context.TODO(), data)

					}
					if !iter.Next() {
						logger.Info("导出完成" + item.Symbol)
						doneArr = append(doneArr, make(chan bool))
					}
				}
				if iter.Err() != nil {
					logger.Error(iter.Err())
				}
			}
		}

	}

	if arrLen == len(doneArr) {
		os.NewFile(1, "init.lock")
	}

}

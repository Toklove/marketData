package main

import (
	"context"
	"fmt"
	restModels "github.com/polygon-io/client-go/rest/models"
	"os"
	"time"
)

func HistoryInit() {
	fmt.Println("开始初始化历史数据")

	//判断是否初始化 创建一个本地LOCK文件
	_, err := os.ReadFile("init.lock")

	if err == nil {
		fmt.Println("已经初始化")
		wg.Done()
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

				//获取三个月行情
				limit := 50000
				order := restModels.Asc
				params := restModels.ListAggsParams{
					Ticker:     item.SymbolHistory,
					From:       restModels.Millis(time.Now().AddDate(0, 0, -7)),
					To:         restModels.Millis(time.Now()),
					Order:      &order,
					Limit:      &limit,
					Timespan:   restModels.Minute,
					Multiplier: 1,
				}

				iter := restClient.AggsClient.ListAggs(context.TODO(), &params)
				logger.Info("开始导出" + item.Symbol)
				for iter.Next() {
					out := iter.Item()
					//保存到mongoDB中
					collection := db.Collection(item.Symbol)

					//根据Symbol和Timestamp判断是否存在
					var result marketData

					//将timestamp转换为int64
					timestamp := time.Time(out.Timestamp).Unix() * 1000

					collection.FindOne(context.TODO(), marketData{Symbol: item.Symbol, Timestamp: timestamp}).Decode(&result)
					//如果存在则不保存
					if result.Symbol == "" {
						//转换成marketData
						var data = marketData{
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
		wg.Done()
	}

}

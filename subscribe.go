package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/url"
	"time"
)

func SubscribeInit() {

	logger.Info("开始订阅")

	//3.1从数据库中获取交易分类

	var marketCategory []MarketCategory

	mysqlDb.Table("market_categories").Find(&marketCategory)

	for _, category := range marketCategory {
		if category.Id == 2 || category.Id == 3 {
			go GetDataByGoMarket(category)
			continue
		}
		go GetDataByCategory(category)
	}
}

func GetDataByGoMarket(category MarketCategory) {
	// TODO 寻找数据接口

	logger.Info("从gomarket获取数据")

	//建立WebSocket连接 wss://api.gomarketes.com:8282/ 并自动短线重连
	u := url.URL{Scheme: "wss", Host: "api.gomarketes.com:8282"}

	//获取交易对
	var markets []Market

	mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

	if len(markets) == 0 {
		return
	}

	tickers := make([]string, len(markets))

	//循环导出交易对
	for _, item := range markets {
		//判断交易对是否存在 如果不存在则创建
		names, _ := db.ListCollectionNames(context.TODO(), nil)

		has := false

		for _, name := range names {
			if name == item.Symbol {
				has = true
				break
			}
		}

		if !has {
			db.CreateCollection(context.Background(), item.Symbol)
			for _, s := range timeList {
				db.CreateCollection(context.Background(), item.Symbol+"_"+s)
			}
		}

		tickers = append(tickers, item.Symbol)
	}

	//将市场Market拼接成{"symbol":"XAGUSD.XAUUSD.XPDUSD.UKOIL.NATGAS.USOIL","type":"price","language":"en_US"}
	var market string
	for _, item := range tickers {
		market += item + "."
	}
	market = market[:len(market)-1]
	var subscribeMsg = fmt.Sprintf(`{"symbol":"%s","type":"price","language":"en_US"}`, market)

	logger.Info("订阅信息")
	logger.Info(subscribeMsg)
	logger.Info("订阅信息")

	var conn *websocket.Conn
	var err error

	for {
		conn, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			logger.Println("dial:", err)
			continue
		}

		conn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg))

		//设置定时任务 每隔30秒发送一次心跳 {"type":"heartbeat","msg":"ping"}
		go func() {
			for {
				time.Sleep(time.Second * 30)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"heartbeat","msg":"ping"}`))
			}
		}()

		break
	}

	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Println("read:", err)
			continue
		}
		//接收信息并解析成数组
		var data []GoMarketData
		err = json.Unmarshal(message, &data)
		if err != nil {
			logger.Println("json:", err)
			continue
		}

		for _, item := range data {
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
				db.CreateCollection(context.TODO(), item.Symbol)
				for _, s := range timeList {
					db.CreateCollection(context.TODO(), item.Symbol+"_"+s)
				}
			}

			//保存到mongoDB中
			collection := db.Collection(item.Symbol)

			//根据Symbol和Timestamp判断是否存在

			timestamp := item.T * 1000

			var result MarketData
			err := collection.FindOne(context.TODO(), MarketData{Symbol: item.Symbol, Timestamp: timestamp}).Decode(&result)

			//如果存在则不保存
			if result.Symbol == "" {
				//转换成marketData
				var data = MarketData{
					Open:      item.Buy,
					High:      item.High,
					Low:       item.Low,
					Close:     item.Close,
					Symbol:    item.Symbol,
					Timestamp: timestamp,
					Volume:    float64(item.Vol),
				}

				_, err = collection.InsertOne(context.TODO(), data)
				if err != nil {
					logger.Info(err)
				}

				//将数据发送到订阅中
				subscribe <- data
			}
		}
	}
}

func GetDataByCategory(category MarketCategory) {

	market, topic := getCategory(category.Name)

	//create a new client
	c, err := polygonws.New(polygonws.Config{
		APIKey: PolygonApiKey,
		Feed:   polygonws.RealTime,
		Market: market,
		Log:    logger,
	})

	logger.Info("连接polygon,当前分类:", category.Name)

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
		return
	}

	tickers := make([]string, len(markets))

	//循环导出交易对
	for _, item := range markets {
		//判断交易对是否存在 如果不存在则创建
		names, _ := db.ListCollectionNames(context.TODO(), nil)

		has := false

		for _, name := range names {
			if name == item.Symbol {
				has = true
				break
			}
		}

		if !has {
			db.CreateCollection(context.Background(), item.Symbol)
			for _, s := range timeList {
				db.CreateCollection(context.Background(), item.Symbol+"_"+s)
			}
		}

		tickers = append(tickers, item.Symbol)
	}

	logger.Info("订阅交易对")
	logger.Info(tickers)

	if err := c.Subscribe(topic, tickers...); err != nil {
		logger.Info(err)
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
				var result MarketData
				collection.FindOne(context.Background(), MarketData{Symbol: out.Pair, Timestamp: out.EndTimestamp}).Decode(&result)

				//如果存在则不保存
				if result.Symbol == "" {
					// 执行查询，获取今日零点起的第一条数据
					now := time.Now()
					midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Unix() * 1000

					// 创建查询条件
					filter := bson.M{
						"timestamp": bson.M{
							"$gte": midnight,
						},
					}

					// 设置查询选项：根据 timestamp 字段排序，并且只返回一条记录
					opts := options.FindOne().SetSort(bson.D{{"timestamp", 1}})

					// 执行查询
					var firstData MarketData
					err = collection.FindOne(context.TODO(), filter, opts).Decode(&firstData)
					if err != nil {
						logger.Info(err)
					}

					// 执行查询，获取今日最高close的数据
					var highData MarketData
					err = collection.FindOne(context.TODO(), filter, options.FindOne().SetSort(bson.D{{"close", -1}})).Decode(&highData)
					if err != nil {
						// 处理错误
						logger.Info(err)
					}

					// 执行查询，获取今日最高close的数据
					var lowData MarketData
					err = collection.FindOne(context.TODO(), filter, options.FindOne().SetSort(bson.D{{"close", 1}})).Decode(&lowData)
					if err != nil {
						// 处理错误
						logger.Info(err)
					}

					open := out.Open
					high := out.High
					low := out.Close

					if firstData.Open != 0 {
						open = firstData.Open
					}

					if highData.Open != 0 {
						high = highData.High
					}

					if lowData.Open != 0 {
						low = lowData.Low
					}

					//转换成marketData
					var data = MarketData{
						Open:      open,
						High:      high,
						Low:       low,
						Close:     out.Close,
						Symbol:    out.Pair,
						Timestamp: out.EndTimestamp,
						Volume:    out.Volume,
					}

					_, err = collection.InsertOne(context.Background(), data)
					if err != nil {
						logger.Info(err)
					}

					//将数据发送到订阅中
					subscribe <- data
				}
			}
		}
	}
}

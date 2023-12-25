package main

import (
	"context"
	"fmt"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	polygonRest "github.com/polygon-io/client-go/rest"
	restModels "github.com/polygon-io/client-go/rest/models"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net/http"
	"os"
	"time"
)

var (
	client  *mongo.Client
	err     error
	db      *mongo.Database
	mysqlDb *gorm.DB
)

type MarketCategory struct {
	Id   int    `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

type Market struct {
	Id     int    `gorm:"column:id"`
	Symbol string `gorm:"column:symbol"`
}

type marketData struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Volume    float64 `json:"volume"`
}

func main() {

	os.Setenv("POLYGON_API_KEY", "KsgzThK2sJ2GY3NVGyt2GSsKuWRpoSPp")

	//用fastHttp启动websocket服务
	app := fiber.New()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		// Websocket logic
		for {
			mtype, msg, err := c.ReadMessage()
			if err != nil {
				break
			}

			//定时发送Ping请求 若无回应则关闭连接
			go func() {
				for {
					err = c.WriteMessage(websocket.PingMessage, []byte("ping"))
					if err != nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
			}()

			logger.Printf("Read: %s", msg)

			err = c.WriteMessage(mtype, msg)
			if err != nil {
				break
			}
		}
		logger.Println("Error:", err)
	}))

	//1.建立连接
	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(5*time.Second)); err != nil {
		fmt.Println("连接失败")
		fmt.Print(err.Error())
		return
	}

	// 检查连接
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		logger.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	//2.选择数据库 marketData
	db = client.Database("marketData")

	//3.从Mysql中获取需要保存行情的交易对
	mysqlDb, err = gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",                                                                            // 驱动名称
		DSN:        "root:123456@tcp(localhost:3306)/smartTrade?charset=utf8&parseTime=True&loc=Local", // data source name, refer https://github.com/go-sql-driver/mysql#dsn-data-source-name
	}), &gorm.Config{})

	//3.1从数据库中获取交易分类

	var marketCategory []MarketCategory

	mysqlDb.Table("market_categories").Find(&marketCategory)

	for _, category := range marketCategory {
		market := polygonws.Stocks
		topic := polygonws.StocksSecAggs

		if category.Name == "Cryptos" {
			market = polygonws.Crypto
			topic = polygonws.CryptoSecAggs
		} else if category.Name == "Forex" {
			market = polygonws.Forex
			topic = polygonws.ForexMinAggs
		} else if category.Name == "Indices" {
			market = polygonws.Indices
		}
		//create a new client
		c, err := polygonws.New(polygonws.Config{
			APIKey: "KsgzThK2sJ2GY3NVGyt2GSsKuWRpoSPp",
			Feed:   polygonws.RealTime,
			Market: market,
			Log:    logger,
		})

		hc := http.Client{} // some custom HTTP client

		rest := polygonRest.NewWithClient("KsgzThK2sJ2GY3NVGyt2GSsKuWRpoSPp", &hc)

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
				db.CreateCollection(context.TODO(), item.Symbol)

				//获取半年行情
				limit := 50000
				order := restModels.Asc
				params := restModels.ListAggsParams{
					Ticker:     "X:ETHUSD",
					From:       restModels.Millis(time.Now().AddDate(0, -6, 0)),
					To:         restModels.Millis(time.Now()),
					Order:      &order,
					Limit:      &limit,
					Timespan:   restModels.Minute,
					Multiplier: 1,
				}
				iter := rest.AggsClient.ListAggs(context.TODO(), &params)
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
					}
				}
				if iter.Err() != nil {
					logger.Error(iter.Err())
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
					collection.FindOne(context.Background(), marketData{Symbol: out.Pair, Timestamp: out.EndTimestamp}).Decode(&result)
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
						collection.InsertOne(context.Background(), data)
					}
				}
			}
		}

	}

	logger.Fatal(app.Listen(":3000"))

}

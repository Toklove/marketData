package main

import (
	"context"
	"fmt"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"os"
	"time"
)

var (
	client     *mongo.Client
	err        error
	db         *mongo.Database
	collection *mongo.Collection
	mysqlDb    *gorm.DB
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

	os.Setenv("POLYGON_API_KEY", "RtG8Z5y9vQEeBIAKWpGbfIUSGJiXbGEg")

	//用fastHttp启动websocket服务
	app := fiber.New()

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

			log.Printf("Read: %s", msg)

			err = c.WriteMessage(mtype, msg)
			if err != nil {
				break
			}
		}
		log.Println("Error:", err)
	}))

	//1.建立连接
	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(5*time.Second)); err != nil {
		fmt.Print(err)
		return
	}

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
			topic = polygonws.ForexSecAggs
		} else if category.Name == "Indices" {
			market = polygonws.Indices
		}

		// create a new client
		c, err := polygonws.New(polygonws.Config{
			APIKey: os.Getenv("POLYGON_API_KEY"),
			Feed:   polygonws.RealTime,
			Market: market,
		})

		if err != nil {
			log.Fatal(err)
		}

		// connect to the server
		if err := c.Connect(); err != nil {
			log.Println(err)
			return
		}

		defer c.Close()
		var markets []Market

		mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

		tickers := make([]string, len(markets))

		//循环导出交易对
		for _, item := range markets {
			tickers = append(tickers, item.Symbol)
		}

		//使用协程来分批次获取数据
		go func() {
			if err := c.Subscribe(topic, tickers...); err != nil {
				log.Fatal(err)
			}

			for {
				select {
				case err := <-c.Error():
					log.Fatal(err)
				case out, more := <-c.Output():
					if !more {
						return
					}
					switch out.(type) {
					case models.EquityAgg:
						out := out.(models.EquityAgg)
						collectionName := out.Symbol
						//保存到mongoDB中
						collection = db.Collection(collectionName)

						//根据Symbol和Timestamp判断是否存在
						var result marketData
						collection.FindOne(context.Background(), marketData{Symbol: out.Symbol, Timestamp: out.EndTimestamp}).Decode(&result)
						//如果存在则不保存
						if result.Symbol == "" {
							//转换成marketData
							var data = marketData{
								Open:      out.Open,
								High:      out.High,
								Low:       out.Low,
								Close:     out.Close,
								Symbol:    out.Symbol,
								Timestamp: out.EndTimestamp,
								Volume:    out.Volume,
							}
							collection.InsertOne(context.Background(), data)
							//根据秒级数据生成分钟级数据
							collection.Aggregate(context.Background(), mongo.Pipeline{})

						}
					}
				}
			}
		}()
	}

	log.Fatal(app.Listen(":3000"))

}

package main

import (
	"github.com/gofiber/fiber/v2"
	polygonRest "github.com/polygon-io/client-go/rest"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
	"net/http"
)

var (
	client        *mongo.Client
	err           error
	db            *mongo.Database
	mysqlDb       *gorm.DB
	logger        *logrus.Logger
	PolygonApiKey string
	restClient    *polygonRest.Client
	hc            http.Client
	app           *fiber.App
	//wg            sync.WaitGroup
	subscribe       chan MarketData
	updateSubscribe chan MarketData
	p               fastjson.Parser
)

var timeList = []string{"1M", "5M", "15M", "30M", "1H", "1D"}

func main() {

	PolygonApiKey = "KsgzThK2sJ2GY3NVGyt2GSsKuWRpoSPp"
	//PolygonApiKey = os.Getenv("POLYGON_API_KEY")

	LoggerInit()

	DatabaseInit()

	//不获取历史数据 直接订阅
	//go HistoryInit()

	//新建一个管道用于接收订阅数据
	subscribe = make(chan MarketData)
	updateSubscribe = make(chan MarketData)

	go SubscribeInit()

	//开启N个协程 定时处理数据 处理1M 5M 15M 30M 1H 1D数据的生成
	go HistoryDataInit()

	FiberInit()
}

package main

import (
	"github.com/gofiber/fiber/v2"
	polygonRest "github.com/polygon-io/client-go/rest"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
	"net/http"
	"sync"
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
	wg            sync.WaitGroup
	subscribe     chan marketData
)

func main() {

	PolygonApiKey = "KsgzThK2sJ2GY3NVGyt2GSsKuWRpoSPp"
	//PolygonApiKey = os.Getenv("POLYGON_API_KEY")

	LoggerInit()

	DatabaseInit()

	//等待历史数据初始化完成
	wg.Add(1)
	go HistoryInit()
	wg.Wait()

	//新建一个管道用于接收订阅数据
	subscribe = make(chan marketData)

	FiberInit()

	go SubscribeInit()

}

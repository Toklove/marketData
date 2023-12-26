package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

func DatabaseInit() {
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
}

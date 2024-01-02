package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"time"
)

type TimeSeries struct {
	mu sync.Mutex
	md map[time.Duration][]MarketData
}

func HistoryDataInit() {
	intervals := []time.Duration{
		1 * time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
		24 * time.Hour,
	}

	var marketCategory []MarketCategory
	var markets []Market

	mysqlDb.Table("market_categories").Find(&marketCategory)

	for _, category := range marketCategory {

		if category.Id == 2 || category.Id == 3 || category.Id == 4 {
			continue
		}

		mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

		ts := &TimeSeries{}

		for _, item := range markets {
			for _, interval := range intervals {
				go ts.startGoroutine(item.Symbol, interval)
			}
		}
	}

	// Keep the main function running
	select {}
}

func formatDuration(d time.Duration) string {
	totalMinutes := int(d.Minutes())
	switch {
	case totalMinutes < 60:
		return fmt.Sprintf("%dM", totalMinutes)
	case totalMinutes == 60:
		return "1H"
	case totalMinutes == 24*60:
		return "1D"
	default:
		return fmt.Sprintf("%dH", totalMinutes/60)
	}
}

func MsgDuration(t string) time.Duration {
	switch t {
	case "1M":
		return 1 * time.Minute
	case "5M":
		return 5 * time.Minute
	case "15M":
		return 15 * time.Minute
	case "30M":
		return 30 * time.Minute
	case "1H":
		return 1 * time.Hour
	case "1D":
		return 24 * time.Hour
	default:
		return 0
	}
}

func (ts *TimeSeries) startGoroutine(symbol string, d time.Duration) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			var timeSymbol = symbol + "_" + formatDuration(d)

			// Fetch data from MongoDB
			data := fetchDataFromMongoDB(d, symbol, timeSymbol)

			// Process the data for this duration
			marketData := ts.CalculateMarket(data)

			_, err := db.Collection(timeSymbol).InsertOne(context.Background(), marketData)
			if err != nil {
				logger.Error(err)
			}

			// Send the processed data to the channel
			subscribe <- marketData
		}
	}
}

func fetchDataFromMongoDB(d time.Duration, symbol string, timeSymbol string) []MarketData {
	c := db.Collection(symbol)

	// Define the time range
	now := time.Now()
	start := now.Add(-d)

	// Fetch the data
	var result []MarketData

	// 创建查询条件
	filter := bson.M{
		"timestamp": bson.M{
			"$gte": start.Unix() * 1000,
			"$lt":  now.Unix() * 1000,
		},
	}

	// 执行查询
	cur, err := c.Find(context.TODO(), filter)
	if err != nil {
		logger.Fatal(err)
	}

	// 遍历并打印查询结果
	defer cur.Close(context.TODO())
	for cur.Next(context.TODO()) {
		var data MarketData
		err := cur.Decode(&data)
		if err != nil {
			logger.Fatal(err)
		}
		data.Symbol = timeSymbol

		result = append(result, data)
	}

	if err := cur.Err(); err != nil {
		logger.Fatal(err)
	}

	return result
}

func (ts *TimeSeries) CalculateMarket(data []MarketData) MarketData {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if len(data) == 0 {
		return MarketData{}
	}

	//遍历data获取最小的low
	var low = 0.0
	var high = 0.0
	for _, d := range data {
		if low == 0.0 {
			low = d.Low
		}

		if high == 0.0 {
			high = d.High
		}
		if d.Low < low {
			low = d.Low
		}
		if d.High > high {
			high = d.High
		}
	}

	market := MarketData{
		Open:      data[0].Open,
		High:      high,
		Low:       low,
		Close:     data[len(data)-1].Close,
		Timestamp: data[len(data)-1].Timestamp,
		Volume:    data[len(data)-1].Volume,
		Symbol:    data[len(data)-1].Symbol,
	}

	for _, d := range data[1:] {
		if d.High > market.High {
			market.High = d.High
		}
		if d.Low < market.Low {
			market.Low = d.Low
		}
	}

	return market
}

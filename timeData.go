package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

func HistoryDataInit() {

	var marketCategory []MarketCategory
	var markets []Market

	mysqlDb.Table("market_categories").Find(&marketCategory)

	for _, category := range marketCategory {

		mysqlDb.Table("markets").Where("category_id = ?", category.Id).Find(&markets)

		for _, item := range markets {
			go startGoroutine(item.Symbol)
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

func aggregateData(secondData []MarketData, duration time.Duration) map[int64]*MarketData {
	aggregatedData := make(map[int64]*MarketData)

	for _, data := range secondData {
		dataTime := time.Unix(0, data.Timestamp*int64(time.Millisecond))
		roundedTime := dataTime.Truncate(duration)

		if _, ok := aggregatedData[roundedTime.UnixMilli()]; !ok {
			aggregatedData[roundedTime.UnixMilli()] = &MarketData{Timestamp: roundedTime.UnixMilli(), Open: data.Open, High: data.High, Low: data.Low, Close: data.Close, Symbol: data.Symbol, Volume: data.Volume}
		} else {
			if data.High > aggregatedData[roundedTime.UnixMilli()].High {
				aggregatedData[roundedTime.UnixMilli()].High = data.High
			}
			if data.Low < aggregatedData[roundedTime.UnixMilli()].Low {
				aggregatedData[roundedTime.UnixMilli()].Low = data.Low
			}
			aggregatedData[roundedTime.UnixMilli()].Close = data.Close
			aggregatedData[roundedTime.UnixMilli()].Volume += data.Volume
		}
	}

	return aggregatedData
}

func createMatrix(durations []time.Duration) map[time.Duration][]int64 {
	matrix := make(map[time.Duration][]int64)

	for _, duration := range durations {
		var timeStamps []int64
		for t := time.Now().Truncate(24 * time.Hour); t.Before(time.Now().Add(24 * time.Hour)); t = t.Add(duration) {
			timeStamps = append(timeStamps, t.UnixMilli())
		}
		matrix[duration] = timeStamps
	}

	return matrix
}

func startGoroutine(symbol string) {

	//var timeSymbol = symbol + "_" + formatDuration(d)

	// Simulate second-level market data
	var secondData []MarketData

	for {
		select {
		case data := <-updateSubscribe:
			if data.Symbol != symbol {
				continue
			}

			// Process the data for this duration
			durations := []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour, 24 * time.Hour}
			matrix := createMatrix(durations)

			// Generate a new data point
			secondData = append(secondData, data)

			// Aggregate the data for each duration
			for _, duration := range durations {
				aggregatedData := aggregateData(secondData, duration)

				// Print the aggregated data for the current time stamp in the matrix
				for _, timeStamp := range matrix[duration] {
					if _, ok := aggregatedData[timeStamp]; ok {
						//将duration转换为 1M 5M格式
						result := *aggregatedData[timeStamp]
						timeSymbol := result.Symbol + "_" + formatDuration(duration)
						result.Symbol = timeSymbol
						fmt.Printf("Aggregated data for duration %v and time stamp %v: %v\n", formatDuration(duration), timeStamp, result)
						subscribe <- result
					}
				}
			}
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

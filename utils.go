package main

import polygonws "github.com/polygon-io/client-go/websocket"

func getCategory(name string) (polygonws.Market, polygonws.Topic) {
	market := polygonws.Stocks
	topic := polygonws.StocksMinAggs

	if name == "Cryptos" {
		market = polygonws.Crypto
		topic = polygonws.CryptoSecAggs
	} else if name == "Forex" {
		market = polygonws.Forex
		topic = polygonws.ForexSecAggs
	} else if name == "Indices" {
		market = polygonws.Indices
	}

	return market, topic
}

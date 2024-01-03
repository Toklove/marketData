package main

type MarketCategory struct {
	Id   int    `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

type Market struct {
	Id            int    `gorm:"column:id"`
	Symbol        string `gorm:"column:symbol"`
	SymbolHistory string `gorm:"column:symbol_history"`
}

type MarketData struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Volume    float64 `json:"volume"`
}

type GoMarketData struct {
	Buy        float64 `json:"buy"`
	Close      float64 `json:"close"`
	High       float64 `json:"high"`
	Increase   float64 `json:"increase"`
	Logo       string  `json:"logo"`
	Low        float64 `json:"low"`
	Name       string  `json:"name"`
	OpenStatus bool    `json:"open_status"`
	Sell       float64 `json:"sell"`
	Symbol     string  `json:"symbol"`
	T          int64   `json:"t"`
	Vol        int     `json:"vol"`
}

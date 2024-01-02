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

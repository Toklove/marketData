package main

import (
	"encoding/json"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"log"
	"sync"
)

type wsClient struct {
	isClosing bool
	mu        sync.Mutex
}

type Message struct {
	Type   string `json:"type"`
	Market string `json:"market"`
	Time   string `json:"time"`
}

type ChannelMessage struct {
	Message string          `json:"message"`
	Channel *websocket.Conn `json:"channel"`
}

var clients = make(map[*websocket.Conn]*wsClient)

var register = make(chan *websocket.Conn)
var broadcast = make(chan ChannelMessage)
var unregister = make(chan *websocket.Conn)
var group = make(map[string][]*websocket.Conn)

func runHub() {
	for {
		select {
		case connection := <-register:
			clients[connection] = &wsClient{}
			log.Println("connection registered")

		case content := <-broadcast:
			message, connection := content.Message, content.Channel
			log.Println("message received:", message)

			//解析message为json 参数:type,market,time
			var msg Message
			mess, err := p.Parse(message)
			if err != nil {
				log.Println("error parsing message:", err)
				continue

			}

			msg.Type = string(mess.GetStringBytes("type"))
			msg.Market = string(mess.GetStringBytes("market"))
			msg.Time = string(mess.GetStringBytes("time"))

			logger.Info(msg.Type)

			//判断是否为订阅消息
			if msg.Type == "subscribe" {
				//获取交易对
				market := msg.Market
				//获取时间
				time := msg.Time

				//判断是否存在该交易对
				var marketObj Market
				mysqlDb.Table("markets").Where("symbol = ?", market).First(&marketObj)

				if marketObj.Id == 0 {
					log.Println("交易对不存在")
					continue
				}

				var key string

				if time == "" {
					key = market
				} else {
					key = market + "_" + time
				}

				//根据订阅信息分组
				group[key] = append(group[key], connection)

				//第一次连接时发送历史数据
				//获取历史数据 最新1000条

			} else if msg.Type == "unsubscribe" {
				//获取交易对
				market := msg.Market
				//获取时间
				time := msg.Time

				//判断是否存在该交易对
				var marketObj Market
				mysqlDb.Table("markets").Where("symbol = ?", market).First(&marketObj)

				if marketObj.Id == 0 {
					log.Println("交易对不存在")
					continue
				}

				var key string

				if time == "" {
					key = market
				} else {
					key = market + "_" + time
				}

				//取消订阅 从group中删除
				for i, conn := range group[key] {
					//如果存在则删除
					if conn == connection {
						group[key] = append(group[key][:i], group[key][i+1:]...)
						break
					}
				}
			} else if msg.Type == "ticker" {
				data := fetchDataFromMongoDB(MsgDuration(msg.Time), msg.Market, msg.Market)
				//将data转换成json字符串并发送
				fastjson, err := json.Marshal(data)
				if err != nil {
					log.Println("error marshalling data:", err)
					continue
				}
				connection.WriteMessage(websocket.TextMessage, fastjson)
			}

		case connection := <-unregister:
			// Remove the client from the hub
			delete(clients, connection)

			log.Println("connection unregistered")
		case data := <-subscribe:
			//logger.Info("发送订阅信息")
			//获取交易对
			market := data.Symbol
			for _, conn := range group[market] {
				jsonData, err := json.Marshal(data)
				if err != nil {
					log.Println("error marshalling data:", err)
					continue
				}
				err = conn.WriteMessage(websocket.TextMessage, jsonData)
				if err != nil {
					return
				}
			}
		}
	}
}

func FiberInit() {

	//用fastHttp启动websocket服务
	app = fiber.New()

	app.Use(func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) { // Returns true if the client requested upgrade to the WebSocket protocol
			return c.Next()
		}
		return c.SendStatus(fiber.StatusUpgradeRequired)
	})

	go runHub()

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		// When the function returns, unregister the client and close the connection
		defer func() {
			unregister <- c
			c.Close()
		}()

		// Register the client
		register <- c

		for {
			messageType, message, err := c.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Println("read error:", err)
				}

				return // Calls the deferred function, i.e. closes the connection on error
			}

			if messageType == websocket.TextMessage {
				// Broadcast the received message and connection to the hub

				broadcast <- ChannelMessage{Channel: c, Message: string(message)}
			} else {
				log.Println("websocket message received of type", messageType)
			}
		}
	}))

	logger.Fatal(app.Listen(":3000"))
}

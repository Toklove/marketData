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
			err := json.Unmarshal([]byte(message), &msg)
			if err != nil {
				log.Println("error parsing JSON:", err)
				continue
			}

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

				key := market + "_" + time

				//根据订阅信息分组
				group[key] = append(group[key], connection)

			} else if msg.Type == "unsubscribe" {
				//获取交易对
				market := msg.Market
				//获取时间
				//time := msg.Time

				//判断是否存在该交易对
				var marketObj Market
				mysqlDb.Table("markets").Where("symbol = ?", market).First(&marketObj)

				if marketObj.Id == 0 {
					log.Println("交易对不存在")
					continue
				}

				//key := market + "_" + time

				//取消订阅 从group中删除
				//group[key] = remove(group[key], connection)
			}

		case connection := <-unregister:
			// Remove the client from the hub
			delete(clients, connection)

			log.Println("connection unregistered")
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

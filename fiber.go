package main

import (
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"time"
)

func FiberInit() {

	//用fastHttp启动websocket服务
	app = fiber.New()

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		// Websocket logic
		for {
			mtype, msg, err := c.ReadMessage()
			if err != nil {
				break
			}

			//定时发送Ping请求 若无回应则关闭连接
			go func() {
				for {
					err = c.WriteMessage(websocket.PingMessage, []byte("ping"))
					if err != nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
			}()

			logger.Printf("Read: %s", msg)

			err = c.WriteMessage(mtype, msg)
			if err != nil {
				break
			}
		}
		logger.Println("Error:", err)
	}))

	logger.Fatal(app.Listen(":3000"))
}

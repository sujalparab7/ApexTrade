package Websocket

import (
	"log"
	"net/http"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func Ws(c *gin.Context) {
	url := "wss://stream.binance.com:9443/ws/btcusdt@trade"
	clientConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Could not connect to frontend")
		return
	}
	defer clientConn.Close()
	log.Println("Frontend client connected!")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("Failed to connect to url%v", err)
		return
	}
	defer conn.Close()
	log.Println("Connected to Binance succesfully")
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("websocket connection broken ")
			break
		}
		log.Printf("Binance Data %s", message)
		err = clientConn.WriteMessage(messageType, message)
		if err != nil {
			log.Println("Frontend client disconnected:", err)
			break
		}
	}
}
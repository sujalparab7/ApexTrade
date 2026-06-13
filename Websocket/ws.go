package Websocket

import (
	"ApexTrade/database"
	"log"
	"net/http"
	"encoding/json"
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

func Ws (c *gin.Context) {
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
		var Tick database.TradeInfo
		err=json.Unmarshal(message,&Tick)
		if err!=nil{
			log.Println("Unable to parse the incoming JSON:",err)
			continue
		}
		result:=database.DB.Create(&Tick)
		if result.Error!=nil{
			log.Println("Failed to save it to database",result.Error)
			return
		}else{
			log.Printf("Vault saved! BTC Price:%s | Quantity:%s",Tick.Price,Tick.Quantity)
		}
	}
}
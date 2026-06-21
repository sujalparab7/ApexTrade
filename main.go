package main

import (
	"encoding/json"
	"context"
	"fmt"
	"log"
	"net/http"
	"ApexTrade/Websocket"
	"ApexTrade/database"
	"ApexTrade/routeguide"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BinanceTick struct{
	EventID   int64  `json:"t"`
	EventType string `json:"e"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
}

func startAIIngestionPipeline(){
	grpcConn,err:=grpc.NewClient("localhost:50051",grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err!=nil{
		log.Printf("Failed to connect to Python server")
		return
	}
	defer grpcConn.Close()
	grpcClient:=routeguide.NewRouteGuideClient(grpcConn)
	stream,err:=grpcClient.SendData(context.Background())
	if err!=nil{
		log.Printf("Error opening the grpc stream, %v",err)
		return
	}

	go func(){
		for{
			response,err:=stream.Recv()
			if err!=nil{
				return
			}
			log.Printf("AI says action code:%d",response.GetAction())
		}
	}()
	binanceURL := "wss://stream.binance.com:9443/ws/btcusdt@trade"
	wsConn, _, err := websocket.DefaultDialer.Dial(binanceURL, nil)
	if err != nil {
		log.Printf("Binance connection failed: %v", err)
		return
	}
	defer wsConn.Close()
	for{
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			break
		}

		var liveTick BinanceTick
		json.Unmarshal(message, &liveTick)

		tickData := &routeguide.InputData{
			ID:        liveTick.EventID,
			EventType: liveTick.EventType,
			Price:     liveTick.Price,
			Quantity:  liveTick.Quantity,
		}

		stream.Send(tickData)
		fmt.Printf("BTC Price is:%s\n",liveTick.Price)
	}
}

func main(){
	database.ConnectDB()
	go startAIIngestionPipeline()
	r:=gin.Default()
	r.LoadHTMLFiles("index.html")

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})

	r.GET("/ws", Websocket.Ws)
	fmt.Println("Gin server running on port 8080")
	r.Run(":8080")
}
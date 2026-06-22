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
	"time"
	"google.golang.org/grpc/credentials/insecure"
	"strconv"
	"sync"
)

type BinanceTick struct{
	EventID   int64  `json:"t"`
	EventType string `json:"e"`
	EventTime int64 `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
}

type StreamPayload struct{
	Timestamp int64 `json:"timestamp"`
	Price float64 `json:"price"`
	Action int32 `json:"action"`
}

var latestPrice float64
var priceMutex sync.RWMutex

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
			priceMutex.RLock()
			currentP := latestPrice
			priceMutex.RUnlock()

			payload := StreamPayload{
				Timestamp: time.Now().Unix(),
				Price:     currentP,
				Action:    response.GetAction(),
			}

			Websocket.Broadcast <- payload
			log.Printf("AI Action Code: %d | Pushed to WS Broadcast Channel", response.GetAction())	
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
		if err := json.Unmarshal(message, &liveTick); err != nil {
			log.Printf("Skipping corrupted tick, JSON parse error: %v | Raw: %s\n", err, string(message))
			continue 
		}
		parsedPrice, err := strconv.ParseFloat(liveTick.Price, 64)
		if err == nil {
			priceMutex.Lock()
			latestPrice = parsedPrice
			priceMutex.Unlock()
		}
		database.DB.Create(&database.TradeInfo{
			EventTime: time.UnixMilli(liveTick.EventTime), 
			Price:     liveTick.Price,
			Quantity:  liveTick.Quantity,
			Action:    "SYSTEM_TICK",
			Source:    "BINANCE_TICK",
		})
		tickData := &routeguide.InputData{
			ID:        liveTick.EventID,
			EventType: liveTick.EventType,
			Price:     liveTick.Price,
			Quantity:  liveTick.Quantity,
		}

		if err := stream.Send(tickData); err != nil {
			log.Println("Failed to send data to Python AI Engine:", err)
		}
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
	r.POST("/api/manual-trade", func(c *gin.Context) {
		var req struct {
			Action string  `json:"action"`
			Price  float64 `json:"price"`
		}
		
		if err := c.ShouldBindJSON(&req); err == nil {
			database.DB.Create(&database.TradeInfo{
				EventTime: time.Now(),
				Price:     fmt.Sprintf("%f", req.Price),
				Action:    req.Action,
				Source:    "MANUAL_USER",
			})
			fmt.Printf("User executed manual %s at %f\n", req.Action, req.Price)
			c.JSON(http.StatusOK, gin.H{"status": "success"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload"})
		}
	})
	fmt.Println("Gin server running on port 8080")
	r.Run(":8080")
}
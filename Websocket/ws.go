package Websocket

import (
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, 
}

var clients = make(map[*websocket.Conn]bool)
var clientsMutex sync.Mutex

var Broadcast = make(chan interface{})

func init() {
	go func() {
		for {
			msg := <-Broadcast 
			
			clientsMutex.Lock()
			for client := range clients {
				err := client.WriteJSON(msg)
				if err != nil {
					client.Close()
					delete(clients, client) 
				}
			}
			clientsMutex.Unlock()
		}
	}()
}

func Ws(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}
	
	clientsMutex.Lock()
	clients[ws] = true
	clientsMutex.Unlock()
	
	log.Println("New client connected to the Broadcast stream")
}
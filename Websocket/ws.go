package Websocket

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// 1. The Client wrapper
type Client struct {
	conn *websocket.Conn
	send chan interface{} // Dedicated buffer for this specific client
}

// 2. The Engine Hub
type Hub struct {
	clients    map[*Client]bool
	Broadcast  chan interface{}
	register   chan *Client
	unregister chan *Client
}

var EngineHub = Hub{
	Broadcast:  make(chan interface{}),
	register:   make(chan *Client),
	unregister: make(chan *Client),
	clients:    make(map[*Client]bool),
}

func init() {
	go EngineHub.run()
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.Broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
					// Success: The message is in the client's queue
				default:
					// Fallback: If the channel is full (client is lagging), drop them immediately.
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

// 4. The Dedicated Writer Pump
func (c *Client) writePump() {
	defer c.conn.Close()
	for {
		msg, ok := <-c.send
		if !ok {
			return
		}

		c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err := c.conn.WriteJSON(msg)
		if err != nil {
			return
		}
	}
}

func Ws(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}

	client := &Client{conn: ws, send: make(chan interface{}, 256)}
	EngineHub.register <- client

	go client.writePump()

	log.Println("New client connected to the optimized broadcast stream")
}
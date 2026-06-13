package main

import (
	"ApexTrade/Websocket"
	"ApexTrade/database"
	"net/http"
	"github.com/gin-gonic/gin"
)

func main(){
	database.ConnectDB()
	r:=gin.Default()
	r.LoadHTMLFiles("index.html")

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})

	r.GET("/ws", Websocket.Ws)

	r.Run(":8080")
}
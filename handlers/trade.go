package handlers

import (
	"ApexTrade/database"
	"fmt"
	"net/http"
	"time"
	"github.com/gin-gonic/gin"
)

func TradeHandler(c *gin.Context) {
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
}

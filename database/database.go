package database

import (
	"log"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

var DB *gorm.DB

type TradeInfo struct {
	ID        int    `gorm:"primaryKey"`
	EventType string `json:"e" gorm:"-"` // Shield to catch "trade", but ignored by DB!
	EventTime int64  `json:"E"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
}

func ConnectDB() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Could not connect to database")
		return
	}
	log.Println("Connected to Database")
	err = db.AutoMigrate(&TradeInfo{})
	if err != nil {
		log.Println("Could not insert the struct inside the database")
		return
	}
	log.Println("Successfully inserted the info the database")
	DB = db
}

package database

import (
	"log"
	"gorm.io/driver/postgres"
	"time"
	"gorm.io/gorm"
	"os"
	"fmt"
	"github.com/joho/godotenv"
)

var DB *gorm.DB

type TradeInfo struct {
	ID        int    `gorm:"primaryKey"`
	EventType string `json:"e" gorm:"-"`//This will be ignored by gorm ,here to just take the input  
	EventTime time.Time `gorm:"not null"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	Action string
	Source string
}

func ConnectDB() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: No .env file found. Relying on system environment variables.")
	}
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")     
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", 
    dbHost, dbPort, dbUser, dbPass, dbName)
	db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
	if err != nil {
		log.Fatalf("Could not connect to database")
		return
	}
	log.Println("Connected to Timescale Database")
	err = db.AutoMigrate(&TradeInfo{})
	if err != nil {
		log.Println("Could not insert the struct inside the database,Migraion failed",err)
		return
	}
	db.Exec("SELECT create_hypertable('trade_infos', 'event_time', if_not_exists => TRUE);")
	log.Println("Successfully configured TimescaleDB Hypertable")
	DB = db
}

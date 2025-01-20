package main

import (
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

var (
	kafkaProducer sarama.SyncProducer
	kafkaTopic    string
)

func initKafka() {
	broker := os.Getenv("KAFKA_BROKER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")

	if broker == "" || kafkaTopic == "" {
		log.Fatal("KAFKA_BROKER and KAFKA_TOPIC must be set in the environment variables")
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}

	kafkaProducer = producer
}

func publishHandler(c *gin.Context) {
	// Log request details
	log.Printf("Received request: %s %s", c.Request.Method, c.Request.URL.Path)

	// Check method
	if c.Request.Method != "POST" {
		c.JSON(http.StatusMethodNotAllowed, gin.H{
			"error":  "Method not allowed. Use POST",
			"method": c.Request.Method,
		})
		return
	}

	var request struct {
		Message string `json:"message" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		log.Printf("Error binding JSON: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	log.Printf("Attempting to send message to Kafka, length: %d", len(request.Message))

	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(request.Message),
	}

	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		log.Printf("Error sending message to Kafka: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	log.Printf("Message sent successfully: partition=%d, offset=%d", partition, offset)
	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"topic":     kafkaTopic,
	})
}

func main() {
	// Set Gin mode
	gin.SetMode(gin.ReleaseMode)

	initKafka()
	//defer kafkaProducer.Close()

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// Add OPTIONS handler for CORS
	router.OPTIONS("/publish", func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		c.Status(http.StatusOK)
	})

	// Main publish endpoint
	router.POST("/publish", publishHandler)

	// Add a health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Add a catch-all handler for 404s
	router.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{
			"error":  "Route not found",
			"path":   c.Request.URL.Path,
			"method": c.Request.Method,
		})
	})

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s...", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

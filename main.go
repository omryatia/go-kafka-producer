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

func main() {
	initKafka()
	defer kafkaProducer.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/publish", func(c *gin.Context) {
		log.Printf("Received message: %s", c.Request.Body)
		var request struct {
			Message string `json:"message" binding:"required"`
		}

		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		message := &sarama.ProducerMessage{
			Topic: kafkaTopic,
			Value: sarama.StringEncoder(request.Message),
		}

		partition, offset, err := kafkaProducer.SendMessage(message)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "success",
			"partition": partition,
			"offset":    offset,
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

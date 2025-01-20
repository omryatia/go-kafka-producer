package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

var (
	kafkaProducer sarama.SyncProducer
	kafkaTopic    string
	brokerAddr    string
	producerMutex sync.Mutex
)

func getKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	return config
}

func ensureKafkaConnection() error {
	producerMutex.Lock()
	defer producerMutex.Unlock()

	if kafkaProducer != nil {
		// Test the connection
		_, _, err := kafkaProducer.SendMessage(&sarama.ProducerMessage{
			Topic: kafkaTopic,
			Value: sarama.StringEncoder("ping"),
		})
		if err == nil {
			return nil
		}
		log.Printf("Kafka connection test failed: %v", err)
		kafkaProducer.Close()
	}

	// Reconnect
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, getKafkaConfig())
	if err != nil {
		return err
	}

	kafkaProducer = producer
	log.Printf("Successfully connected to Kafka broker: %s", brokerAddr)
	return nil
}

func initKafka() error {
	brokerAddr = os.Getenv("KAFKA_BROKER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")

	if brokerAddr == "" || kafkaTopic == "" {
		return fmt.Errorf("KAFKA_BROKER and KAFKA_TOPIC must be set")
	}

	log.Printf("Initializing Kafka connection to broker: %s, topic: %s", brokerAddr, kafkaTopic)
	return ensureKafkaConnection()
}

func publishHandler(c *gin.Context) {
	var request struct {
		Message string `json:"message" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	// Ensure Kafka connection is active
	if err := ensureKafkaConnection(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  "Failed to connect to Kafka: " + err.Error(),
		})
		return
	}

	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(request.Message),
	}

	producerMutex.Lock()
	partition, offset, err := kafkaProducer.SendMessage(message)
	producerMutex.Unlock()

	if err != nil {
		log.Printf("Error sending message to Kafka: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"topic":     kafkaTopic,
	})
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	if err := initKafka(); err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	router.POST("/publish", publishHandler)
	router.GET("/health", func(c *gin.Context) {
		err := ensureKafkaConnection()
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "error",
				"error":  "Kafka connection failed: " + err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

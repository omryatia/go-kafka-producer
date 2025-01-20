package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

var (
	kafkaProducer sarama.SyncProducer
	producerOnce  sync.Once
	producerErr   error
	kafkaTopic    string
)

type MessageEvent struct {
	Text string `json:"text"`
}

func getKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// Network configurations
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Net.MaxOpenRequests = 5

	// Metadata configurations
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = time.Second
	config.Metadata.RefreshFrequency = time.Second * 10

	// Explicitly set the broker address
	config.Metadata.Full = true

	return config
}

func InitKafkaProducer() (sarama.SyncProducer, error) {
	producerOnce.Do(func() {
		config := getKafkaConfig()

		// Get Kafka brokers from environment
		brokers := strings.Split(os.Getenv("KAFKA_BROKER"), ",")
		if len(brokers) == 0 || (len(brokers) == 1 && brokers[0] == "") {
			producerErr = fmt.Errorf("KAFKA_BROKER environment variable not set")
			return
		}

		log.Printf("Attempting to connect to Kafka brokers: %v", brokers)

		kafkaTopic := os.Getenv("KAFKA_TOPIC")
		if kafkaTopic == "" {
			producerErr = fmt.Errorf("KAFKA_TOPIC environment variable not set")
			return
		}

		log.Printf("Using Kafka topic: %s", kafkaTopic)

		// Create new sync producer
		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			producerErr = fmt.Errorf("failed to create Kafka producer: %v", err)
			return
		}

		kafkaProducer = producer
		log.Printf("Kafka producer initialized successfully, brokers: %v", brokers)
	})

	return kafkaProducer, producerErr
}

func SendToKafka(topic string, event MessageEvent) (int32, int64, error) {
	producer, err := InitKafkaProducer()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to initialize Kafka producer: %v", err)
	}

	// Convert event to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal message event: %v", err)
	}

	// Create message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(jsonData),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to send message to Kafka: %v", err)
	}

	log.Printf("Message sent to Kafka, topic: %s, partition: %d, offset: %d", topic, partition, offset)

	return partition, offset, nil
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

	event := MessageEvent{Text: request.Message}
	partition, offset, err := SendToKafka(kafkaTopic, event)
	if err != nil {
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

func healthCheck(c *gin.Context) {
	producer, err := InitKafkaProducer()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error",
			"error":  fmt.Sprintf("Kafka producer not initialized: %v", err),
		})
		return
	}

	// Try to send a test message
	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder("health_check"),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error",
			"error":  fmt.Sprintf("Kafka health check failed: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func main() {
	// Initialize Kafka producer early to catch configuration errors
	_, err := InitKafkaProducer()
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer func() {
		if kafkaProducer != nil {
			if err := kafkaProducer.Close(); err != nil {
				log.Printf("Error closing Kafka producer: %v", err)
			}
		}
	}()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.POST("/publish", publishHandler)
	router.GET("/health", healthCheck)

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

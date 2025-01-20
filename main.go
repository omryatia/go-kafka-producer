package main

import (
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

func createKafkaProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := getKafkaConfig()

	log.Printf("Attempting to connect to Kafka brokers: %v", brokerList)

	// Try multiple times to connect
	var producer sarama.SyncProducer
	var err error

	for i := 0; i < 3; i++ {
		producer, err = sarama.NewSyncProducer(brokerList, config)
		if err == nil {
			return producer, nil
		}
		log.Printf("Failed to connect to Kafka (attempt %d/3): %v", i+1, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}

	return nil, fmt.Errorf("failed to connect to Kafka after 3 attempts: %v", err)
}

func initKafka() error {
	brokerAddr = os.Getenv("KAFKA_BROKER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")

	if brokerAddr == "" || kafkaTopic == "" {
		return fmt.Errorf("KAFKA_BROKER and KAFKA_TOPIC must be set")
	}

	// Split broker string in case there are multiple brokers
	brokers := strings.Split(brokerAddr, ",")
	log.Printf("Initializing Kafka connection to brokers: %v, topic: %s", brokers, kafkaTopic)

	producer, err := createKafkaProducer(brokers)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}

	kafkaProducer = producer
	log.Printf("Successfully connected to Kafka")
	return nil
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

	producerMutex.Lock()
	defer producerMutex.Unlock()

	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(request.Message),
	}

	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		log.Printf("Error sending message to Kafka: %v", err)

		// Try to reconnect once on failure
		if strings.Contains(err.Error(), "connection refused") {
			if producer, err := createKafkaProducer([]string{brokerAddr}); err == nil {
				kafkaProducer = producer
				partition, offset, err = kafkaProducer.SendMessage(message)
			}
		}

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status": "error",
				"error":  fmt.Sprintf("Failed to send message: %v", err),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"topic":     kafkaTopic,
	})
}

func healthCheck(c *gin.Context) {
	producerMutex.Lock()
	defer producerMutex.Unlock()

	if kafkaProducer == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error",
			"error":  "Kafka producer not initialized",
		})
		return
	}

	// Try to send a test message
	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder("health_check"),
	}

	_, _, err := kafkaProducer.SendMessage(message)
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
	gin.SetMode(gin.ReleaseMode)

	if err := initKafka(); err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}
	defer kafkaProducer.Close()

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

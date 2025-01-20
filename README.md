# README

## Go Kafka Producer

This repository contains a Go application that acts as a Kafka producer. It provides an API to send messages to a Kafka topic.

## Features

- Uses the `github.com/IBM/sarama` library for Kafka interaction.
- REST API with Gin framework.
- Dockerized for easy deployment.

## Requirements

- Go 1.23+
- Docker
- Railway (for deployment)
- A running Kafka instance (e.g., Kafka on Railway).

## File Structure

```plaintext
/go-kafka-producer
├── Dockerfile
├── README.md
├── go.mod
├── go.sum
├── main.go
└── .env.example
```

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```dotenv
KAFKA_BROKER=<Your Kafka Broker Address>
KAFKA_TOPIC=<Your Kafka Topic>
API_PORT=8080
```

For example:

```dotenv
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=test-topic
API_PORT=8080
```

## Running Locally

1. Clone the repository:

   ```bash
   git clone https://github.com/omryatia/go-kafka-producer.git
   cd go-kafka-producer
   ```

2. Install dependencies:

   ```bash
   go mod tidy
   ```

3. Run the application:

   ```bash
   go run main.go
   ```

4. Send a test request:

   ```bash
   curl -X POST -H "Content-Type: application/json" \
        -d '{"message":"Hello Kafka"}' \
        http://localhost:8080/publish
   ```

## Docker Setup

### Build and Run Locally

1. Build the Docker image:

   ```bash
   docker build -t go-kafka-producer .
   ```

2. Run the container:

   ```bash
   docker run --env-file .env -p 8080:8080 go-kafka-producer
   ```

### Deploy to Railway

1. Push the repository to GitHub.
2. Connect the repository to your Railway project.
3. Add the following environment variables to Railway:

   - `KAFKA_BROKER`
   - `KAFKA_TOPIC`
   - `API_PORT`

4. Deploy the project on Railway.

## API Endpoints

### POST `/publish`

#### Request Body

```json
{
  "message": "Your message here"
}
```

#### Response

```json
{
  "status": "success",
  "message": "Message sent to Kafka successfully"
}
```

## References

- [Sarama GitHub](https://github.com/Shopify/sarama)
- [Gin Framework](https://gin-gonic.com/)

---

Happy coding! 🎉


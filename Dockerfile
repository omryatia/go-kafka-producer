# Use the official Go image
FROM golang:1.23 as builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the application source code
COPY . .

# Build the application
RUN go build -o main .

# Use a minimal base image
FROM gcr.io/distroless/base-debian11

# Set working directory
WORKDIR /

# Copy the compiled binary from the builder
COPY --from=builder /app/main .

# Expose the application port
EXPOSE 8080

# Run the application
CMD ["./main"]

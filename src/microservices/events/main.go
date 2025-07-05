package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var (
	logger   = logrus.New()
	producer sarama.SyncProducer
	consumer sarama.Consumer
)

type Event struct {
	Type      string                 `json:"type"`
	UserID    int                    `json:"user_id,omitempty"`
	MovieID   int                    `json:"movie_id,omitempty"`
	Amount    float64                `json:"amount,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type EventRequest struct {
	Type    string                 `json:"type"`
	UserID  int                    `json:"user_id,omitempty"`
	MovieID int                    `json:"movie_id,omitempty"`
	Amount  float64                `json:"amount,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

func init() {
	// Configure logger
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	// Initialize Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	var err error
	producer, err = sarama.NewSyncProducer([]string{kafkaBrokers}, config)
	if err != nil {
		logger.Fatalf("Failed to create Kafka producer: %v", err)
	}

	// Initialize Kafka consumer
	consumer, err = sarama.NewConsumer([]string{kafkaBrokers}, nil)
	if err != nil {
		logger.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Start consuming events
	go consumeEvents()

	logger.Info("Events service initialized successfully")
}

func main() {
	r := mux.NewRouter()

	// Health check endpoint
	r.HandleFunc("/health", healthHandler).Methods("GET")

	// Events endpoints
	r.HandleFunc("/api/events", createEventHandler).Methods("POST")
	r.HandleFunc("/api/events", getEventsHandler).Methods("GET")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	logger.Infof("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":          "healthy",
		"service":         "Events Service",
		"kafka_connected": producer != nil,
	}
	json.NewEncoder(w).Encode(response)
}

func createEventHandler(w http.ResponseWriter, r *http.Request) {
	var eventReq EventRequest
	if err := json.NewDecoder(r.Body).Decode(&eventReq); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		Type:      eventReq.Type,
		UserID:    eventReq.UserID,
		MovieID:   eventReq.MovieID,
		Amount:    eventReq.Amount,
		Timestamp: time.Now(),
		Data:      eventReq.Data,
	}

	// Determine topic based on event type
	var topic string
	switch event.Type {
	case "user":
		topic = "user-events"
	case "movie":
		topic = "movie-events"
	case "payment":
		topic = "payment-events"
	default:
		topic = "general-events"
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		logger.Errorf("Failed to marshal event: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Send to Kafka
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		logger.Errorf("Failed to send message to Kafka: %v", err)
		http.Error(w, "Failed to process event", http.StatusInternalServerError)
		return
	}

	logger.Infof("Event sent to Kafka - Topic: %s, Partition: %d, Offset: %d", topic, partition, offset)

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	response := map[string]interface{}{
		"message": "Event created successfully",
		"event":   event,
		"kafka": map[string]interface{}{
			"topic":     topic,
			"partition": partition,
			"offset":    offset,
		},
	}
	json.NewEncoder(w).Encode(response)
}

func getEventsHandler(w http.ResponseWriter, r *http.Request) {
	// Simple endpoint to return recent events (in a real scenario, you'd query a database)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"message": "Events service is running",
		"endpoints": []string{
			"POST /api/events - Create a new event",
			"GET /health - Health check",
		},
	}
	json.NewEncoder(w).Encode(response)
}

func consumeEvents() {
	// Subscribe to all event topics
	topics := []string{"user-events", "movie-events", "payment-events", "general-events"}

	for _, topic := range topics {
		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			logger.Errorf("Failed to start consumer for topic %s: %v", topic, err)
			continue
		}
		defer partitionConsumer.Close()

		go func(topic string, pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				var event Event
				if err := json.Unmarshal(message.Value, &event); err != nil {
					logger.Errorf("Failed to unmarshal event from topic %s: %v", topic, err)
					continue
				}

				logger.Infof("Consumed event from topic %s: %+v", topic, event)
			}
		}(topic, partitionConsumer)
	}

	logger.Info("Started consuming events from all topics")
}

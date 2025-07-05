package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var (
	logger   = logrus.New()
	producer sarama.SyncProducer
	consumer sarama.Consumer
)

// Event structures according to API specification
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Username  string  `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType *string `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
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
	r.HandleFunc("/api/events/health", healthHandler).Methods("GET")

	// Events endpoints according to API specification
	r.HandleFunc("/api/events/movie", createMovieEventHandler).Methods("POST")
	r.HandleFunc("/api/events/user", createUserEventHandler).Methods("POST")
	r.HandleFunc("/api/events/payment", createPaymentEventHandler).Methods("POST")

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
		"status": true,
	}
	json.NewEncoder(w).Encode(response)
}

func createMovieEventHandler(w http.ResponseWriter, r *http.Request) {
	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        uuid.New().String(),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"movie_id":    movieEvent.MovieID,
			"title":       movieEvent.Title,
			"action":      movieEvent.Action,
			"user_id":     movieEvent.UserID,
			"rating":      movieEvent.Rating,
			"genres":      movieEvent.Genres,
			"description": movieEvent.Description,
		},
	}

	// Send to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "movie-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to process event", http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	json.NewEncoder(w).Encode(response)
}

func createUserEventHandler(w http.ResponseWriter, r *http.Request) {
	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        uuid.New().String(),
		Type:      "user",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"user_id":   userEvent.UserID,
			"username":  userEvent.Username,
			"email":     userEvent.Email,
			"action":    userEvent.Action,
			"timestamp": userEvent.Timestamp,
		},
	}

	// Send to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "user-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to process event", http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	json.NewEncoder(w).Encode(response)
}

func createPaymentEventHandler(w http.ResponseWriter, r *http.Request) {
	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        uuid.New().String(),
		Type:      "payment",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"payment_id":  paymentEvent.PaymentID,
			"user_id":     paymentEvent.UserID,
			"amount":      paymentEvent.Amount,
			"status":      paymentEvent.Status,
			"timestamp":   paymentEvent.Timestamp,
			"method_type": paymentEvent.MethodType,
		},
	}

	// Send to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "payment-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to process event", http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	json.NewEncoder(w).Encode(response)
}

func consumeEvents() {
	// Subscribe to all event topics
	topics := []string{"user-events", "movie-events", "payment-events"}

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

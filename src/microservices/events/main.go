package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
	Username  string  `json:"username"`
	Email     string  `json:"email"`
}

type MovieEvent struct {
	MovieID     int		`json:"movie_id"`
	Title       string 	`json:"title"`
	Action      string	`json:"action"`
	UserID      int		`json:"user_id"`
}

type PaymentEvent struct {
	PaymentID int     `json:"payment_id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
	MethodType string `json:"method_type"`
}

var (
	kafkaBroker       string
	userTopic         string
	movieTopic        string
	paymentTopic      string
	writer            *kafka.Writer
)

func main() {
	loadEnv()
	initProducer()

	// запускаем consumer'ы
	go startConsumer(userTopic, "user-group")
	go startConsumer(movieTopic, "movie-group")
	go startConsumer(paymentTopic, "payment-group")

	http.HandleFunc("/api/events/health", healthCheck)
	http.HandleFunc("/api/events/user", createUserEvent)
	http.HandleFunc("/api/events/payment", createPaymentEvent)
	http.HandleFunc("/api/events/movie", createMovieEvent)

	server := &http.Server{
		Addr: ":8082",
	}

	go func() {
		log.Println("Events service running on :8082")
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// graceful shutdown
	waitForShutdown(server)
}

func loadEnv() {
	kafkaBroker = getEnv("KAFKA_BROKER", "kafka:9092")
	userTopic = getEnv("USER_TOPIC", "user-events")
	movieTopic = getEnv("MOVIE_TOPIC", "movie-events")
	paymentTopic = getEnv("PAYMENT_TOPIC", "payment-events")
}

func initProducer() {
	writer = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func createUserEvent(w http.ResponseWriter, r *http.Request) {
	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	event := Event{
		Type:      "User",
		Timestamp: time.Now(),
		Payload: userEvent,
	}
	produce(userTopic, event)
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

func createPaymentEvent(w http.ResponseWriter, r *http.Request) {
	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	
	event := Event{
		Type:      "Payment",
		Timestamp: time.Now(),
		Payload: paymentEvent,
	}
	produce(paymentTopic, event)
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

func createMovieEvent(w http.ResponseWriter, r *http.Request) {
	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}	
	
	event := Event{
		Type:      "Movie",
		Timestamp: time.Now(),
		Payload: movieEvent,
	}
	produce(movieTopic, event)
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

func produce(topic string, event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Println("Marshal error:", err)
		return
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Key:   []byte(event.Type),
			Value: data,
		},
	)

	if err != nil {
		log.Println("Produce error:", err)
		return
	}

	log.Printf("Produced to %s: %s\n", topic, string(data))
}

func startConsumer(topic string, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	log.Printf("Consumer started for topic: %s\n", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Consumer error (%s): %v\n", topic, err)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		processEvent(topic, event)
	}
}

func processEvent(topic string, event Event) {
	log.Printf("Consumed from %s → type=%s payload=%v\n",
		topic, event.Type, event.Payload)
}

func waitForShutdown(server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down...")

	server.Shutdown(context.Background())
	writer.Close()

	log.Println("Shutdown complete")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

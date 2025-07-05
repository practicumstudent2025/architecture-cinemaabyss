package main

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	// Configuration
	monolithURL            string
	moviesServiceURL       string
	eventsServiceURL       string
	gradualMigration       bool
	moviesMigrationPercent int
)

func init() {
	// Configure logger
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	// Load environment variables
	monolithURL = getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	eventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")

	gradualMigration = getEnv("GRADUAL_MIGRATION", "true") == "true"
	moviesMigrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")
	moviesMigrationPercent, _ = strconv.Atoi(moviesMigrationPercentStr)

	logger.Infof("Proxy service initialized with configuration:")
	logger.Infof("Monolith URL: %s", monolithURL)
	logger.Infof("Movies Service URL: %s", moviesServiceURL)
	logger.Infof("Events Service URL: %s", eventsServiceURL)
	logger.Infof("Gradual Migration: %v", gradualMigration)
	logger.Infof("Movies Migration Percent: %d%%", moviesMigrationPercent)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	r := mux.NewRouter()

	// Health check endpoint
	r.HandleFunc("/health", healthHandler).Methods("GET")

	// API routes with Strangler Fig pattern
	r.HandleFunc("/api/movies", moviesHandler).Methods("GET", "POST")
	r.HandleFunc("/api/movies/health", moviesHealthHandler).Methods("GET")

	// Other routes - proxy to monolith
	r.HandleFunc("/api/users", proxyToMonolith).Methods("GET", "POST")
	r.HandleFunc("/api/payments", proxyToMonolith).Methods("GET", "POST")
	r.HandleFunc("/api/subscriptions", proxyToMonolith).Methods("GET", "POST")
	r.HandleFunc("/api/events", proxyToEvents).Methods("GET", "POST")

	// Catch-all for other routes
	r.PathPrefix("/").HandlerFunc(proxyToMonolith)

	port := getEnv("PORT", "8000")
	logger.Infof("Starting proxy service on port %s", port)
	logger.Fatal(http.ListenAndServe(":"+port, r))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":                   "healthy",
		"service":                  "Strangler Fig Proxy",
		"gradual_migration":        gradualMigration,
		"movies_migration_percent": moviesMigrationPercent,
	}
	json.NewEncoder(w).Encode(response)
}

func moviesHealthHandler(w http.ResponseWriter, r *http.Request) {
	// Proxy to movies service health endpoint
	resp, err := http.Get(moviesServiceURL + "/api/movies/health")
	if err != nil {
		logger.Errorf("Error calling movies service health: %v", err)
		http.Error(w, "Movies service health check failed", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func moviesHandler(w http.ResponseWriter, r *http.Request) {
	if !gradualMigration {
		// If gradual migration is disabled, always use monolith
		proxyToMonolith(w, r)
		return
	}

	// Use Strangler Fig pattern for movies endpoint
	random := rand.Intn(100)

	if random < moviesMigrationPercent {
		// Route to movies microservice
		logger.Infof("Routing movies request to microservice (random: %d, threshold: %d)", random, moviesMigrationPercent)
		proxyToMoviesService(w, r)
	} else {
		// Route to monolith
		logger.Infof("Routing movies request to monolith (random: %d, threshold: %d)", random, moviesMigrationPercent)
		proxyToMonolith(w, r)
	}
}

func proxyToMoviesService(w http.ResponseWriter, r *http.Request) {
	// Create URL for movies service
	targetURL, err := url.Parse(moviesServiceURL)
	if err != nil {
		logger.Errorf("Error parsing movies service URL: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify request
	originalPath := r.URL.Path
	r.URL.Host = targetURL.Host
	r.URL.Scheme = targetURL.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = targetURL.Host

	// Log the request
	logger.Infof("Proxying to movies service: %s %s", r.Method, originalPath)

	// Serve the request
	proxy.ServeHTTP(w, r)
}

func proxyToEvents(w http.ResponseWriter, r *http.Request) {
	// Create URL for events service
	targetURL, err := url.Parse(eventsServiceURL)
	if err != nil {
		logger.Errorf("Error parsing events service URL: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify request
	originalPath := r.URL.Path
	r.URL.Host = targetURL.Host
	r.URL.Scheme = targetURL.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = targetURL.Host

	// Log the request
	logger.Infof("Proxying to events service: %s %s", r.Method, originalPath)

	// Serve the request
	proxy.ServeHTTP(w, r)
}

func proxyToMonolith(w http.ResponseWriter, r *http.Request) {
	// Create URL for monolith
	targetURL, err := url.Parse(monolithURL)
	if err != nil {
		logger.Errorf("Error parsing monolith URL: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify request
	originalPath := r.URL.Path
	r.URL.Host = targetURL.Host
	r.URL.Scheme = targetURL.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = targetURL.Host

	// Log the request
	logger.Infof("Proxying to monolith: %s %s", r.Method, originalPath)

	// Serve the request
	proxy.ServeHTTP(w, r)
}

func init() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())
}

package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	gradualMigration     bool
	moviesMigrationPercent int
	monolithURL          *url.URL
	moviesServiceURL     *url.URL
)

func main() {
	loadEnv()
	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/", router)

	log.Println("Gateway running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func loadEnv() {
	var err error

	gradualMigration, err = strconv.ParseBool(getEnv("GRADUAL_MIGRATION", "false"))
	if err != nil {
		log.Fatal("Invalid GRADUAL_MIGRATION value")
	}

	moviesMigrationPercent, err = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || moviesMigrationPercent < 0 || moviesMigrationPercent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be 0-100")
	}

	monolithURL, err = url.Parse(getEnv("MONOLITH_URL", "http://monolith:8080"))
	if err != nil {
		log.Fatal("Invalid MONOLITH_URL")
	}

	moviesServiceURL, err = url.Parse(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	if err != nil {
		log.Fatal("Invalid MOVIES_SERVICE_URL")
	}

	log.Printf("GRADUAL_MIGRATION=%v", gradualMigration)
	log.Printf("MOVIES_MIGRATION_PERCENT=%d", moviesMigrationPercent)
	log.Printf("MONOLITH_URL=%s", monolithURL)
	log.Printf("MOVIES_SERVICE_URL=%s", moviesServiceURL)
}

func router(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Health check
	if path == "/api/movies/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
	}

	// Movies API routing
	if strings.HasPrefix(path, "/api/movies") {
		proxyToMovies(w, r)
		return
	}

	// Everything else → monolith
	proxy(monolithURL, w, r)
}

func proxyToMovies(w http.ResponseWriter, r *http.Request) {
	target := monolithURL

	if gradualMigration {
		random := rand.Intn(100)
		if random < moviesMigrationPercent {
			target = moviesServiceURL
		}
	}

	log.Printf("target=%s", target)

	proxy(target, w, r)
}

func proxy(target *url.URL, w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(target)

	// preserve original host
	r.Host = target.Host
	proxy.ServeHTTP(w, r)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
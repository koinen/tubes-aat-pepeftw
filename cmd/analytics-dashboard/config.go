package main

import (
	"errors"
	"os"
	"time"
)

type config struct {
	port          string
	aggregatorURL string
	internalKey   string
	timeout       time.Duration
}

func loadConfig() (config, error) {
	timeout, err := time.ParseDuration(getenv("AGGREGATOR_TIMEOUT", "6s"))
	if err != nil || timeout <= 0 {
		return config{}, errors.New("AGGREGATOR_TIMEOUT must be a positive duration such as 6s")
	}
	cfg := config{
		port:          getenv("PORT", "8080"),
		aggregatorURL: getenv("AGGREGATOR_URL", "http://aggregator:8080"),
		internalKey:   os.Getenv("INTERNAL_API_KEY"),
		timeout:       timeout,
	}
	if cfg.internalKey == "" {
		return config{}, errors.New("INTERNAL_API_KEY is required")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

package main

import (
	"errors"
	"os"
	"strings"
)

type config struct {
	port    string
	brokers []string
	topic   string
	groupID string
}

func loadConfig() (config, error) {
	cfg := config{
		port:    getenv("PORT", "8080"),
		brokers: splitList(os.Getenv("KAFKA_BROKERS")),
		topic:   getenv("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
		groupID: getenv("KAFKA_GROUP_ID", "audit-log"),
	}
	if len(cfg.brokers) == 0 {
		return config{}, errors.New("KAFKA_BROKERS is required")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

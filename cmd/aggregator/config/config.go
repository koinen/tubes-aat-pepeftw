package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port           string
	InternalAPIKey string
	SeedFixtures   bool
	DatabaseURL    string
	KafkaBrokers   []string
	HazardTopic    string
	RelayInterval  time.Duration
}

func Load() (Config, error) {
	seed, err := strconv.ParseBool(getenv("SEED_FIXTURES", "true"))
	if err != nil {
		return Config{}, errors.New("SEED_FIXTURES must be true or false")
	}

	dbURL, err := databaseURL()
	if err != nil {
		return Config{}, err
	}

	interval, err := time.ParseDuration(getenv("RELAY_INTERVAL", "1s"))
	if err != nil || interval <= 0 {
		return Config{}, errors.New("RELAY_INTERVAL must be a positive duration such as 1s or 500ms")
	}

	cfg := Config{
		Port:           getenv("PORT", "8080"),
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
		SeedFixtures:   seed,
		DatabaseURL:    dbURL,
		KafkaBrokers:   splitList(os.Getenv("KAFKA_BROKERS")),
		HazardTopic:    getenv("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
		RelayInterval:  interval,
	}

	if cfg.InternalAPIKey == "" {
		return Config{}, errors.New("INTERNAL_API_KEY is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}

	return cfg, nil
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

func databaseURL() (string, error) {
	for _, key := range []string{"DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"} {
		if os.Getenv(key) == "" {
			return "", fmt.Errorf("%s is required", key)
		}
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD")),
		Host:     net.JoinHostPort(os.Getenv("DB_HOST"), getenv("DB_PORT", "5432")),
		Path:     "/" + os.Getenv("DB_NAME"),
		RawQuery: "sslmode=disable",
	}
	return u.String(), nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

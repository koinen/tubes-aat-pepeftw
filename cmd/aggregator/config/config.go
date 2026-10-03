package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
)

type Config struct {
	Port           string
	InternalAPIKey string
	SeedFixtures   bool
	DatabaseURL    string
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

	cfg := Config{
		Port:           getenv("PORT", "8080"),
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
		SeedFixtures:   seed,
		DatabaseURL:    dbURL,
	}

	if cfg.InternalAPIKey == "" {
		return Config{}, errors.New("INTERNAL_API_KEY is required")
	}

	return cfg, nil
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

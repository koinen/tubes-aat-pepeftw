package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	Port           string
	InternalAPIKey string
	SeedFixtures   bool
}

func Load() (Config, error) {
	seed, err := strconv.ParseBool(getenv("SEED_FIXTURES", "true"))
	if err != nil {
		return Config{}, errors.New("SEED_FIXTURES must be true or false")
	}

	cfg := Config{
		Port:           getenv("PORT", "8080"),
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
		SeedFixtures:   seed,
	}

	if cfg.InternalAPIKey == "" {
		return Config{}, errors.New("INTERNAL_API_KEY is required")
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

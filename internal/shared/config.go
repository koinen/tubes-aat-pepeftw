package shared

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// InitEnv attempts to load .env files from standard locations.
// It will try loading service-specific `.env` first, then root `.env`.
// It suppresses errors so that in Docker / production environments where env vars
// are set externally, the app continues smoothly.
func InitEnv() {
	// Try loading local .env, then root .env if running from subdirectory
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../.env")
}

// GetEnv retrieves the value of the environment variable named by the key.
// If the variable is not set or is empty, fallback is returned.
func GetEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// GetEnvInt retrieves an integer environment variable, returning fallback if absent/invalid.
func GetEnvInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return fallback
	}
	return val
}

// GetEnvBool retrieves a boolean environment variable, returning fallback if absent/invalid.
func GetEnvBool(key string, fallback bool) bool {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		return fallback
	}
	return val
}

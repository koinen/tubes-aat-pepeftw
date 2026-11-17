package satellite

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Enabled     bool
	URL         string
	Token       string
	Lateness    time.Duration
	BatchSize   int
	BatchWait   time.Duration
	IdleTimeout time.Duration
}

func LoadConfig() (Config, error) {
	cfg := Config{
		Enabled: os.Getenv("SATELLITE_ENABLED") == "true",
		URL:     getenv("SATELLITE_URL", "https://satellite.naufarrel.dev"),
		Token:   os.Getenv("SATELLITE_TOKEN"),
	}

	var err error
	if cfg.Lateness, err = duration("SATELLITE_LATENESS", 6*time.Hour); err != nil {
		return cfg, err
	}
	if cfg.BatchWait, err = duration("SATELLITE_BATCH_WAIT", 500*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.IdleTimeout, err = duration("SATELLITE_IDLE_TIMEOUT", 45*time.Second); err != nil {
		return cfg, err
	}
	cfg.BatchSize = 500
	if v := os.Getenv("SATELLITE_BATCH_SIZE"); v != "" {
		if cfg.BatchSize, err = strconv.Atoi(v); err != nil || cfg.BatchSize <= 0 {
			return cfg, errors.New("SATELLITE_BATCH_SIZE must be a positive integer")
		}
	}

	if cfg.Enabled && cfg.Token == "" {
		return cfg, errors.New("SATELLITE_TOKEN is required when SATELLITE_ENABLED=true")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, errors.New(key + " must be a positive duration such as 6h")
	}
	return d, nil
}

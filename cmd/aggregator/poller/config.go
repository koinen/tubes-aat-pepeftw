package poller

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
)

type Config struct {
	Enabled bool

	BMKGURL    string
	BMKGAPIKey string
	PVMBGURL   string
	PVMBGToken string

	// SensorPantaiURL is empty when Sensor Pantai is not deployed; its runner is then not started.
	SensorPantaiURL          string
	SensorPantaiToken        string
	SensorPantaiPollInterval time.Duration
	SensorPantaiTimeout      time.Duration

	PollInterval  time.Duration
	BMKGTimeout   time.Duration
	PVMBGTimeout  time.Duration
	CursorOverlap time.Duration

	BreakerFailures int
	BreakerOpenFor  time.Duration

	TsunamiGrace time.Duration

	// PVMBGAdaptations are the declared rules for breaking PVMBG schema versions (PVMBG_ADAPTATIONS).
	PVMBGAdaptations mapper.VolcanicAdaptations
}

func LoadConfig() (Config, error) {
	cfg := Config{
		BMKGURL:    strings.TrimRight(getenv("BMKG_URL", "http://bmkg-mock:8080"), "/"),
		BMKGAPIKey: os.Getenv("BMKG_API_KEY"),
		PVMBGURL:   strings.TrimRight(getenv("PVMBG_URL", "http://pvmbg-mock:8080"), "/"),
		PVMBGToken: os.Getenv("PVMBG_TOKEN"),

		SensorPantaiURL:   strings.TrimRight(os.Getenv("SENSOR_PANTAI_URL"), "/"),
		SensorPantaiToken: os.Getenv("PANTAI_AUTH_TOKEN"),
	}

	var err error
	if cfg.Enabled, err = strconv.ParseBool(getenv("POLLER_ENABLED", "true")); err != nil {
		return cfg, errors.New("POLLER_ENABLED must be true or false")
	}
	durations := []struct {
		key string
		def time.Duration
		dst *time.Duration
	}{
		{"POLL_INTERVAL", 3 * time.Second, &cfg.PollInterval},
		{"BMKG_TIMEOUT", 1 * time.Second, &cfg.BMKGTimeout},
		{"PVMBG_TIMEOUT", 4 * time.Second, &cfg.PVMBGTimeout},
		{"POLL_CURSOR_OVERLAP", time.Minute, &cfg.CursorOverlap},
		{"BREAKER_OPEN_FOR", 15 * time.Second, &cfg.BreakerOpenFor},
		{"TSUNAMI_GRACE", 30 * time.Second, &cfg.TsunamiGrace},
		{"SENSOR_PANTAI_POLL_INTERVAL", time.Second, &cfg.SensorPantaiPollInterval},
		{"SENSOR_PANTAI_TIMEOUT", 2 * time.Second, &cfg.SensorPantaiTimeout},
	}
	for _, d := range durations {
		v, err := time.ParseDuration(getenv(d.key, d.def.String()))
		if err != nil || v < 0 {
			return cfg, fmt.Errorf("%s must be a non-negative duration such as 3s", d.key)
		}
		*d.dst = v
	}
	if cfg.BreakerFailures, err = strconv.Atoi(getenv("BREAKER_FAILURES", "3")); err != nil || cfg.BreakerFailures < 1 {
		return cfg, errors.New("BREAKER_FAILURES must be a positive integer")
	}

	// Parsed on every pod, poller or not: every pod accepts a replay request and records its rules.
	if cfg.PVMBGAdaptations, err = mapper.ParseVolcanicAdaptations(os.Getenv("PVMBG_ADAPTATIONS")); err != nil {
		return cfg, err
	}

	if !cfg.Enabled {
		return cfg, nil
	}
	switch {
	case cfg.BMKGAPIKey == "":
		return cfg, errors.New("BMKG_API_KEY is required when POLLER_ENABLED=true")
	case cfg.PVMBGToken == "":
		return cfg, errors.New("PVMBG_TOKEN is required when POLLER_ENABLED=true")
	case cfg.PollInterval <= 0 || cfg.BMKGTimeout <= 0 || cfg.PVMBGTimeout <= 0:
		return cfg, errors.New("POLL_INTERVAL, BMKG_TIMEOUT, and PVMBG_TIMEOUT must be positive")
	case cfg.SensorPantaiURL != "" && cfg.SensorPantaiToken == "":
		return cfg, errors.New("PANTAI_AUTH_TOKEN is required when SENSOR_PANTAI_URL is set")
	case cfg.SensorPantaiURL != "" && (cfg.SensorPantaiPollInterval <= 0 || cfg.SensorPantaiTimeout <= 0):
		return cfg, errors.New("SENSOR_PANTAI_POLL_INTERVAL and SENSOR_PANTAI_TIMEOUT must be positive")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

type Config struct {
	Port           string
	InternalAPIKey string
	DatabaseURL    string
	KafkaBrokers   []string
	HazardTopic    string
	RelayInterval  time.Duration
	AuditToken     string
	OpsToken       string
	FastRead       time.Duration
	ShardRead      time.Duration

	// FieldReportsTopic is consumed by every pod into field_reports.
	FieldReportsTopic string
	// UnprocessedTopic is consumed by every pod into unprocessed_messages as INTERNAL.
	UnprocessedTopic string
}

func Load() (Config, error) {
	dbURL, err := DatabaseURL()
	if err != nil {
		return Config{}, err
	}

	interval, err := time.ParseDuration(getenv("RELAY_INTERVAL", "1s"))
	if err != nil || interval <= 0 {
		return Config{}, errors.New("RELAY_INTERVAL must be a positive duration such as 1s or 500ms")
	}

	fastRead, err := positiveDuration("READ_FAST_TIMEOUT", "1500ms")
	if err != nil {
		return Config{}, err
	}
	shardRead, err := positiveDuration("READ_SHARD_TIMEOUT", "2s")
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Port:           getenv("PORT", "8080"),
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
		DatabaseURL:    dbURL,
		KafkaBrokers:   splitList(os.Getenv("KAFKA_BROKERS")),
		HazardTopic:    getenv("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
		RelayInterval:  interval,
		AuditToken:     os.Getenv("AUDIT_TOKEN"),
		OpsToken:       os.Getenv("OPS_TOKEN"),
		FastRead:       fastRead,
		ShardRead:      shardRead,

		FieldReportsTopic: getenv("KAFKA_TOPIC_FIELD_REPORTS", "field-reports"),
		UnprocessedTopic:  getenv("KAFKA_TOPIC_UNPROCESSED", "unprocessed"),
	}

	if cfg.InternalAPIKey == "" {
		return Config{}, errors.New("INTERNAL_API_KEY is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}

	return cfg, nil
}

func positiveDuration(key, fallback string) (time.Duration, error) {
	d, err := time.ParseDuration(getenv(key, fallback))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as %s", key, fallback)
	}
	return d, nil
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

func DatabaseURL() (string, error) {
	for _, key := range []string{"DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"} {
		if os.Getenv(key) == "" {
			return "", fmt.Errorf("%s is required", key)
		}
	}

	query := url.Values{}
	query.Set("sslmode", "disable")
	query.Set("pool_max_conn_lifetime", getenv("DB_POOL_MAX_CONN_LIFETIME", "5m"))
	query.Set("pool_health_check_period", getenv("DB_POOL_HEALTH_CHECK_PERIOD", "15s"))
	query.Set("pool_max_conns", getenv("DB_POOL_MAX_CONNS", "24"))

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD")),
		Host:     net.JoinHostPort(os.Getenv("DB_HOST"), getenv("DB_PORT", "26257")),
		Path:     "/" + os.Getenv("DB_NAME"),
		RawQuery: query.Encode(),
	}
	return u.String(), nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

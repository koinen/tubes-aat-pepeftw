package config

import (
	"fmt"
	"strings"
	"tubes-aat-pepeftw/internal/shared"
)

type Config struct {
	Server      ServerConfig
	Redis       RedisConfig
	Kafka       KafkaConfig
	Aggregator  AggregatorConfig
	Auth        AuthConfig
	RateLimit   RateLimitConfig
	Concurrency ConcurrencyConfig
}

type ServerConfig struct {
	Port string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
}

type KafkaConfig struct {
	Brokers string
	Topic   string
	GroupID string
}

type AggregatorConfig struct {
	BaseURL     string
	InternalKey string
}

type AuthConfig struct {
	JWKSURL     string
	JWTIssuer   string
	JWTAudience string
}

type RateLimitConfig struct {
	Max           int
	WindowSeconds int
}

type ConcurrencyConfig struct {
	MaxRequests int
}

func LoadConfig() (*Config, error) {
	shared.InitEnv()

	serverCfg := ServerConfig{
		Port: shared.GetEnv("PORT", "8080"),
	}

	redisCfg := RedisConfig{
		Host:     shared.GetEnv("REDIS_HOST", "localhost"),
		Port:     shared.GetEnv("REDIS_PORT", "6379"),
		Password: shared.GetEnv("REDIS_PASSWORD", ""),
	}
	if redisCfg.Host == "" || redisCfg.Port == "" {
		return nil, fmt.Errorf("redis host|port is not set")
	}

	kafkaCfg := KafkaConfig{
		Brokers: shared.GetEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:   shared.GetEnv("KAFKA_TOPIC_AUTH_REVOCATIONS", "auth-revocations"),
		GroupID: shared.GetEnv("KAFKA_GROUP_ID", "client-api-revocations"),
	}

	aggBaseURL := strings.TrimRight(shared.GetEnv("AGGREGATOR_URL", "http://localhost:8081"), "/")
	aggInternalKey := shared.GetEnv("INTERNAL_API_KEY", "")
	if aggInternalKey == "" {
		return nil, fmt.Errorf("INTERNAL_API_KEY is required")
	}

	aggCfg := AggregatorConfig{
		BaseURL:     aggBaseURL,
		InternalKey: aggInternalKey,
	}

	jwksURL := shared.GetEnv("AUTH_JWKS_URL", "")
	if jwksURL == "" {
		authURL := strings.TrimRight(shared.GetEnv("AUTH_URL", "http://localhost:8080"), "/")
		jwksURL = authURL + "/.well-known/jwks.json"
	}

	authCfg := AuthConfig{
		JWKSURL:     jwksURL,
		JWTIssuer:   shared.GetEnv("JWT_ISSUER", "auth-service"),
		JWTAudience: shared.GetEnv("JWT_AUDIENCE", "bnpb-ops"),
	}

	rateLimitCfg := RateLimitConfig{
		Max:           shared.GetEnvInt("RATE_LIMIT_MAX", 10),
		WindowSeconds: shared.GetEnvInt("RATE_LIMIT_WINDOW_SECONDS", 60),
	}

	concurrencyCfg := ConcurrencyConfig{
		MaxRequests: shared.GetEnvInt("MAX_CONCURRENT_REQUESTS", 200),
	}

	return &Config{
		Server:      serverCfg,
		Redis:       redisCfg,
		Kafka:       kafkaCfg,
		Aggregator:  aggCfg,
		Auth:        authCfg,
		RateLimit:   rateLimitCfg,
		Concurrency: concurrencyCfg,
	}, nil
}

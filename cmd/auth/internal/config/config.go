package config

import (
	"fmt"
	"os"
	"tubes-aat-pepeftw/internal/shared"
)

type Config struct {
	Server ServerConfig
	Redis  RedisConfig
	Token  TokenConfig
	Kafka  KafkaConfig
}

type ServerConfig struct {
	Port string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
}

type TokenConfig struct {
	AccessTokenTTL  int // in seconds
	RefreshTokenTTL int // in seconds
	PrivateKey      string
	MediaSecret     string
	FieldTeamSecret string
	BnpbOpsSecret   string
	JWTIssuer       string
	JWTAudience     string
	PublicKey       string
}

type KafkaConfig struct {
	Brokers string
	Topic   string
}

func loadKey(path string) (string, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read key from %s: %v", path, err)
	}
	return string(key), nil
}

func LoadConfig() (*Config, error) {
	shared.InitEnv()

	serverCfg := ServerConfig{
		Port: shared.GetEnv("SERVER_PORT", "8080"),
	}

	if serverCfg.Port == "" {
		return nil, fmt.Errorf("server port is not set")
	}

	redisCfg := RedisConfig{
		Host:     shared.GetEnv("REDIS_HOST", "localhost"),
		Port:     shared.GetEnv("REDIS_PORT", "6379"),
		Password: shared.GetEnv("REDIS_PASSWORD", ""),
	}

	if redisCfg.Host == "" || redisCfg.Port == "" || redisCfg.Password == "" {
		return nil, fmt.Errorf("redis host|port|password is not set")
	}

	privateKey, err := loadKey(shared.GetEnv("PRIVATE_KEY_PATH", "./secrets/auth_priv.pem"))
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %v", err)
	}

	publicKey, err := loadKey(shared.GetEnv("PUBLIC_KEY_PATH", "./secrets/auth_pub.pem"))
	if err != nil {
		return nil, fmt.Errorf("failed to load public key: %v", err)
	}

	tokenCfg := TokenConfig{
		PrivateKey:      privateKey,
		PublicKey:       publicKey,
		AccessTokenTTL:  shared.GetEnvInt("ACCESS_TOKEN_TTL", 60),      // seconds
		RefreshTokenTTL: shared.GetEnvInt("REFRESH_TOKEN_TTL", 604800), // seconds (7 days)
		MediaSecret:     shared.GetEnv("MEDIA_SECRET", ""),
		FieldTeamSecret: shared.GetEnv("LAPANGAN_SECRET", ""),
		BnpbOpsSecret:   shared.GetEnv("BNPB_SECRET", ""),
		JWTIssuer:       shared.GetEnv("JWT_ISSUER", "auth-service"),
		JWTAudience:     shared.GetEnv("JWT_AUDIENCE", "bnpb-ops"),
	}

	if tokenCfg.PrivateKey == "" || tokenCfg.PublicKey == "" {
		return nil, fmt.Errorf("JWT private key is not set")
	}

	kafkaCfg := KafkaConfig{
		Brokers: shared.GetEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:   shared.GetEnv("KAFKA_TOPIC", "auth-revocations"),
	}

	return &Config{
		Server: serverCfg,
		Redis:  redisCfg,
		Token:  tokenCfg,
		Kafka:  kafkaCfg,
	}, nil
}

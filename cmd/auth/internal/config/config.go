package config

import (
	"fmt"
	"tubes-aat-pepeftw/internal/shared"
)

type Config struct {
	Server ServerConfig
	Redis  RedisConfig
	JWT    JWTConfig
}

type ServerConfig struct {
	Port string
}

type RedisConfig struct {
	Host string
	Port string
}

type JWTConfig struct {
	PrivateKey string
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
		Host: shared.GetEnv("REDIS_HOST", "localhost"),
		Port: shared.GetEnv("REDIS_PORT", "6379"),
	}

	if redisCfg.Host == "" || redisCfg.Port == "" {
		return nil, fmt.Errorf("redis host or port is not set")
	}

	jwtCfg := JWTConfig{
		PrivateKey: shared.GetEnv("JWT_PRIVATE_KEY", ""),
	}

	if jwtCfg.PrivateKey == "" {
		return nil, fmt.Errorf("JWT private key is not set")
	}

	return &Config{
		Server: serverCfg,
		Redis:  redisCfg,
		JWT:    jwtCfg,
	}, nil
}

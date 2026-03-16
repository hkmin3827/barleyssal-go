package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port int

	Redis RedisConfig
	Kafka KafkaConfig

	External ExternalConfig
	Cache    CacheConfig

	CorsOrigin string
}

type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
}

type KafkaConfig struct {
	Brokers  []string
	ClientID string
}

type ExternalConfig struct {
	WsURL          string
	KisAppKey      string
	KisAppSecret   string
	KisApprovalURL string
	KisTokenURL    string
	KisBaseURL     string
}

type CacheConfig struct {
	PriceTTL int 
	OhlcvTTL int 
}


func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		Port: parseInt(os.Getenv("PORT"), 4000),

		Redis: RedisConfig{
			Host:     envStr("REDIS_HOST", "localhost"),
			Port:     parseInt(os.Getenv("REDIS_PORT"), 6379),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       parseInt(os.Getenv("REDIS_DB"), 1),
		},

		Kafka: KafkaConfig{
			Brokers:  strings.Split(envStr("KAFKA_BROKERS", "localhost:9092"), ","),
			ClientID: "barleyssal-go-market",
		},

		External: ExternalConfig{
			WsURL:          os.Getenv("EXTERNAL_WS_URL"),
			KisAppKey:      os.Getenv("KIS_APP_KEY"),
			KisAppSecret:   os.Getenv("KIS_APP_SECRET"),
			KisApprovalURL: envStr("KIS_APPROVAL_URL", "https://openapivts.koreainvestment.com:29443/oauth2/Approval"),
			KisTokenURL:    envStr("KIS_TOKEN_URL", "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"),
			KisBaseURL:     envStr("KIS_BASE_URL", "https://openapivts.koreainvestment.com:29443"),
		},

		Cache: CacheConfig{
			PriceTTL: parseInt(os.Getenv("PRICE_CACHE_TTL"), 10),
			OhlcvTTL: parseInt(os.Getenv("OHLCV_CACHE_TTL"), 300),
		},

		CorsOrigin: envStr("CORS_ORIGIN", "*"),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

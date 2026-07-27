// Package config loads all application configuration from environment variables.
// No hardcoded URLs, RPCs, or secrets — everything comes from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Config holds all application configuration loaded from the environment.
type Config struct {
	App      AppConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	Gate     GateConfig
	Sui      SuiConfig
	Auth     AuthConfig
	Pinata   PinataConfig
	OTel     OTelConfig
	AWS      AWSConfig
}

type AppConfig struct {
	Env      string `envconfig:"APP_ENV" default:"development"`
	Name     string `envconfig:"APP_NAME" default:"theme-park-nft-engine"`
	Port     int    `envconfig:"APP_PORT" default:"8080"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
}

type PostgresConfig struct {
	Host     string `envconfig:"POSTGRES_HOST" default:"localhost"`
	Port     int    `envconfig:"POSTGRES_PORT" default:"5432"`
	User     string `envconfig:"POSTGRES_USER" default:"themepark"`
	Password string `envconfig:"POSTGRES_PASSWORD" default:"themepark_dev"`
	DB       string `envconfig:"POSTGRES_DB" default:"themepark"`
	SSLMode  string `envconfig:"POSTGRES_SSLMODE" default:"disable"`
	MaxConns int    `envconfig:"POSTGRES_MAX_CONNS" default:"25"`
}

// DSN returns the PostgreSQL connection string.
func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.DB, p.SSLMode)
}

type RedisConfig struct {
	Host           string `envconfig:"REDIS_HOST" default:"localhost"`
	Port           int    `envconfig:"REDIS_PORT" default:"6379"`
	Password       string `envconfig:"REDIS_PASSWORD" default:""`
	DB             int    `envconfig:"REDIS_DB" default:"0"`
	DedupTTLSec    int    `envconfig:"REDIS_DEDUP_TTL_SECONDS" default:"300"`
	GraceWindowSec int    `envconfig:"REDIS_GRACE_WINDOW_SECONDS" default:"5"`
	AggTTLSec      int    `envconfig:"REDIS_AGG_TTL_SECONDS" default:"172800"`
}

// Addr returns the Redis address string.
func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

type KafkaConfig struct {
	Brokers        string `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	TopicRideScans string `envconfig:"KAFKA_TOPIC_RIDE_SCANS" default:"ride-scans"`
	TopicDLQ       string `envconfig:"KAFKA_TOPIC_DLQ" default:"ride-scans-dlq"`
	ConsumerGroup  string `envconfig:"KAFKA_CONSUMER_GROUP" default:"ride-scan-consumers"`
	Partitions     int    `envconfig:"KAFKA_PARTITIONS" default:"6"`
	Replication    int    `envconfig:"KAFKA_REPLICATION" default:"1"`
	RetentionMS    int64  `envconfig:"KAFKA_RETENTION_MS" default:"604800000"`
}

// BrokerList returns the brokers as a slice.
func (k KafkaConfig) BrokerList() []string {
	return strings.Split(k.Brokers, ",")
}

type GateConfig struct {
	HMACSecret        string `envconfig:"HMAC_SECRET"`
	QRRotationSeconds int    `envconfig:"QR_ROTATION_SECONDS" default:"30"`
}

type SuiConfig struct {
	RPCURL            string `envconfig:"SUI_RPC_URL" default:"https://fullnode.testnet.sui.io"`
	Network           string `envconfig:"SUI_NETWORK" default:"testnet"`
	PackageID         string `envconfig:"SUI_PACKAGE_ID"`
	MintCapID         string `envconfig:"SUI_MINTCAP_ID"`
	GasPoolMnemonic   string `envconfig:"SUI_GAS_POOL_MNEMONIC"`
	GasBudget         string `envconfig:"SUI_GAS_BUDGET" default:"100000000"`
	RPCMaxConcurrency int    `envconfig:"SUI_RPC_MAX_CONCURRENCY" default:"5"`
	RPCMaxRetries     int    `envconfig:"SUI_RPC_MAX_RETRIES" default:"5"`
}

type AuthConfig struct {
	GoogleOAuthClientID     string `envconfig:"GOOGLE_OAUTH_CLIENT_ID"`
	GoogleOAuthClientSecret string `envconfig:"GOOGLE_OAUTH_CLIENT_SECRET"`
	EncryptionKey           string `envconfig:"ENCRYPTION_KEY"`
}

type OTelConfig struct {
	ExporterEndpoint string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT" default:"http://localhost:4317"`
	ServiceName       string `envconfig:"OTEL_SERVICE_NAME" default:"theme-park-nft-engine"`
	DDAPIKey         string `envconfig:"DD_API_KEY"`
	DDSite           string `envconfig:"DD_SITE" default:"datadoghq.com"`
}

type PinataConfig struct {
	APIKey    string `envconfig:"PINATA_API_KEY"`
	APISecret string `envconfig:"PINATA_API_SECRET"`
	Gateway   string `envconfig:"PINATA_GATEWAY" default:"https://gateway.pinata.cloud"`
}

type AWSConfig struct {
	Region        string `envconfig:"AWS_REGION" default:"us-east-1"`
	ECRRegistry   string `envconfig:"AWS_ECR_REGISTRY"`
	EC2InstanceID string `envconfig:"AWS_EC2_INSTANCE_ID"`
}

// Load reads configuration from environment variables and an optional .env file.
// It first attempts to load a .env file via viper (for local dev), then reads
// the process environment via envconfig. Process env always wins.
func Load() (*Config, error) {
	// Load .env file if present (local dev convenience). In production, env vars
	// are injected directly by the orchestrator (Docker/AWS).
	v := viper.New()
	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Warn().Err(err).Msg("failed to read .env file, continuing with process env only")
		}
	}
	// Copy viper values into process env so envconfig picks them up, but only
	// if the process env doesn't already have them (process env wins).
	for _, key := range v.AllKeys() {
		if _, exists := os.LookupEnv(strings.ToUpper(key)); !exists {
			_ = os.Setenv(strings.ToUpper(key), v.GetString(key))
		}
	}

	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("envconfig: %w", err)
	}
	return &cfg, nil
}

// MustLoad panics if configuration cannot be loaded.
func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		panic(fmt.Sprintf("failed to load config: %v", err))
	}
	return cfg
}

// String helpers for downstream packages.
func (c *Config) String() string {
	return fmt.Sprintf("Config{App=%s, PG=%s:%d, Redis=%s:%d, Kafka=%s, Sui=%s}",
		c.App.Name, c.Postgres.Host, c.Postgres.Port, c.Redis.Host, c.Redis.Port, c.Kafka.Brokers, c.Sui.Network)
}

// ParseInt helper for ad-hoc env reads.
func ParseInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
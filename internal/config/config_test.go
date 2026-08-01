package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("APP_ENV", "test")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.App.Name != "theme-park-nft-engine" {
		t.Errorf("App.Name = %q, want %q", cfg.App.Name, "theme-park-nft-engine")
	}
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want localhost", cfg.Postgres.Host)
	}
	// R5: Kafka default must match compose host port 29092
	if cfg.Kafka.Brokers != "localhost:29092" {
		t.Errorf("Kafka.Brokers = %q, want localhost:29092 (R5)", cfg.Kafka.Brokers)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "pg.example.com")
	t.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Postgres.Host != "pg.example.com" {
		t.Errorf("Postgres.Host = %q, want pg.example.com", cfg.Postgres.Host)
	}
	if got := cfg.Kafka.BrokerList(); len(got) != 2 || got[0] != "kafka1:9092" {
		t.Errorf("Kafka.BrokerList() = %v, want [kafka1:9092 kafka2:9092]", got)
	}
}

func TestValidate(t *testing.T) {
	t.Setenv("REQUIRED_A", "x")
	_ = os.Unsetenv("REQUIRED_B")

	cfg := &Config{}
	if err := cfg.Validate("REQUIRED_A"); err != nil {
		t.Errorf("Validate(REQUIRED_A) error = %v, want nil", err)
	}
	if err := cfg.Validate("REQUIRED_B"); err == nil {
		t.Error("Validate(REQUIRED_B) = nil, want error (R3 fail-fast)")
	}
}

func TestKafkaProducerAsync(t *testing.T) {
	// Default is sync (false) — load benchmarks rely on per-batch delivery results.
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Kafka.ProducerAsync {
		t.Error("Kafka.ProducerAsync default = true, want false (sync)")
	}

	t.Setenv("KAFKA_PRODUCER_ASYNC", "true")
	cfg2, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg2.Kafka.ProducerAsync {
		t.Error("Kafka.ProducerAsync = false, want true after KAFKA_PRODUCER_ASYNC=true")
	}
}

func TestDSN(t *testing.T) {
	p := PostgresConfig{Host: "h", Port: 1, User: "u", Password: "p", DB: "d", SSLMode: "disable"}
	want := "host=h port=1 user=u password=p dbname=d sslmode=disable"
	if got := p.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}
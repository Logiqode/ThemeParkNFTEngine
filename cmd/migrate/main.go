// Command migrate runs golang-migrate against PostgreSQL using the project's
// config (env vars + .env). Usage:
//
//	go run ./cmd/migrate up        # apply all pending migrations
//	go run ./cmd/migrate down      # roll back all migrations (destructive)
//	go run ./cmd/migrate version   # print current schema version
//	go run ./cmd/migrate force N   # manually set schema_migrations version to N
package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: migrate <up|down|version|force N>")
		os.Exit(1)
	}

	cfg := config.MustLoad()

	db, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("open postgres")
	}
	defer func() { _ = db.Close() }()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatal().Err(err).Msg("init migrate driver")
	}

	// file:// path is relative to the working directory (project root).
	m, err := migrate.NewWithDatabaseInstance("file://migrations", "postgres", driver)
	if err != nil {
		log.Fatal().Err(err).Msg("init migrate instance")
	}

	switch os.Args[1] {
	case "up":
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			log.Fatal().Err(err).Msg("migrate up failed")
		}
		version, dirty, err := m.Version()
		if err != nil {
			log.Fatal().Err(err).Msg("read version")
		}
		log.Info().Uint("version", version).Bool("dirty", dirty).Msg("migrate up complete")

	case "down":
		if err := m.Down(); err != nil && err != migrate.ErrNoChange {
			log.Fatal().Err(err).Msg("migrate down failed")
		}
		log.Info().Msg("migrate down complete")

	case "version":
		version, dirty, err := m.Version()
		if err != nil {
			log.Fatal().Err(err).Msg("read version")
		}
		fmt.Printf("version=%d dirty=%t\n", version, dirty)

	case "force":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: migrate force N")
			os.Exit(1)
		}
		n, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatal().Err(err).Msg("invalid version")
		}
		if err := m.Force(n); err != nil {
			log.Fatal().Err(err).Msg("force failed")
		}
		log.Info().Int("version", n).Msg("forced version")

	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}
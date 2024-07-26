package main

import (
	"errors"
	"fmt"
	"log"
	"msgproc/internal/config"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	cfg := config.MustLoad()

	dbURL := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&x-migrations-table=%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Database,
		cfg.Migrator.MigrationsTable,
	)

	migrationsPath := fmt.Sprintf("file://%s", cfg.Migrator.MigrationsPath)

	m, err := migrate.New(
		migrationsPath,
		dbURL,
	)
	if err != nil {
		log.Fatalf("Failed to create migrate instance: %v\n", err)
	}

	log.Println("Starting migration...")
	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			log.Println("No changes to migrate")
			return
		}
		log.Fatalf("Migration failed: %v\n", err)
	}

	log.Println("Migrations applied successfully")
}

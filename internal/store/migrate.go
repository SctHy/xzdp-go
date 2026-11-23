// Package migrate installs the PostgreSQL schema in transactional, ordered migrations.
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
)

//go:embed pg/*.sql demo.sql
var files embed.FS

func Apply(ctx context.Context, db *sql.DB) error { return Install(ctx, db, false) }

func Install(ctx context.Context, db *sql.DB, demo bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(70751371)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY)"); err != nil {
		return err
	}
	entries, err := files.ReadDir("pg")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var applied bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT FROM schema_migrations WHERE version=$1)", entry.Name()).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		data, err := files.ReadFile("pg/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("%s: %w", entry.Name(), err)
		}
		if demo && entry.Name() == "001_platform.sql" {
			seed, err := files.ReadFile("demo.sql")
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, string(seed)); err != nil {
				return fmt.Errorf("demo data: %w", err)
			}
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations VALUES($1)", entry.Name()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Package migrate applies the embedded forward migrations to both new and existing volumes.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
)

//go:embed sql/*.sql
var files embed.FS

func Apply(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var locked int
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK('xzdp:migrate',10)").Scan(&locked); err != nil {
		return err
	}
	if locked != 1 {
		return fmt.Errorf("migration lock unavailable")
	}
	defer conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK('xzdp:migrate')")
	if _, err = conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations(version VARCHAR(100) PRIMARY KEY)"); err != nil {
		return err
	}
	entries, err := files.ReadDir("sql")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		var applied bool
		if err = conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?)", name).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		data, err := files.ReadFile("sql/" + name)
		if err != nil {
			return err
		}
		for _, stmt := range strings.Split(string(data), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if strings.HasPrefix(stmt, "ALTER TABLE tb_blog ADD COLUMN like_version") {
				var present bool
				if err = conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='tb_blog' AND column_name='like_version')").Scan(&present); err != nil {
					return err
				}
				if present {
					continue
				}
			}
			if strings.HasPrefix(stmt, "ALTER TABLE tb_voucher_order ADD UNIQUE") {
				var present bool
				if err = conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='tb_voucher_order' AND index_name='uq_order_user_voucher')").Scan(&present); err != nil {
					return err
				}
				if present {
					continue
				}
			}
			if _, err = conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s: %w", name, err)
			}
		}
		if _, err = conn.ExecContext(ctx, "INSERT INTO schema_migrations VALUES (?)", name); err != nil {
			return err
		}
	}
	return nil
}

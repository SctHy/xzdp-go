package store

import (
	"context"
	"database/sql"
	"encoding/json"
)

// List serializes named SQL columns without rewriting numeric order IDs in Go.
func List(ctx context.Context, db *sql.DB, query string, args ...any) (json.RawMessage, error) {
	var b []byte
	e := db.QueryRowContext(ctx, "SELECT coalesce(json_agg(q),'[]') FROM ("+query+") q", args...).Scan(&b)
	return b, e
}

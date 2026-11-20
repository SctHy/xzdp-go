// Command dbcopy imports a consistent MySQL snapshot into an empty PostgreSQL schema.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"xzdp/internal/migrate"
)

type field struct {
	name, kind string
	identity   bool
}

func list(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, stmt string, args ...any) ([]string, error) {
	r, e := q.QueryContext(ctx, stmt, args...)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	var vs []string
	for r.Next() {
		var s string
		if e = r.Scan(&s); e != nil {
			return nil, e
		}
		vs = append(vs, s)
	}
	return vs, r.Err()
}
func myIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func transfer(ctx context.Context, src, dst *sql.Tx, table string) error {
	p := pq.QuoteIdentifier(table)
	m := myIdent(table)
	var occupied bool
	if e := dst.QueryRowContext(ctx, "SELECT EXISTS(SELECT FROM "+p+")").Scan(&occupied); e != nil {
		return e
	}
	if occupied {
		return fmt.Errorf("destination table contains rows")
	}
	meta, e := dst.QueryContext(ctx, `SELECT column_name,data_type,is_identity='YES' FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND is_generated='NEVER' ORDER BY ordinal_position`, table)
	if e != nil {
		return e
	}
	var fields []field
	var names, my, pg []string
	for meta.Next() {
		var f field
		if e = meta.Scan(&f.name, &f.kind, &f.identity); e != nil {
			meta.Close()
			return e
		}
		fields = append(fields, f)
		names = append(names, f.name)
		a, b := myIdent(f.name), pq.QuoteIdentifier(f.name)
		switch f.kind {
		case "bigint", "smallint", "integer":
			a = "CAST(" + a + " AS DECIMAL(20,0))"
		case "timestamp with time zone":
			b = "to_char(" + b + " AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
		case "boolean":
			b = "(" + b + "::int)::text"
		case "bytea":
			a = "LOWER(HEX(" + a + "))"
			b = "encode(" + b + ",'hex')"
		}
		my = append(my, a)
		pg = append(pg, b)
	}
	e = meta.Err()
	meta.Close()
	if e != nil {
		return e
	}
	sourceFields, e := list(ctx, src, "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY COLUMN_NAME", table)
	if e != nil {
		return e
	}
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	if !slices.Equal(sorted, sourceFields) {
		return fmt.Errorf("column mismatch: source %v, destination %v", sourceFields, sorted)
	}
	keys, e := list(ctx, src, "SELECT COLUMN_NAME FROM information_schema.key_column_usage WHERE table_schema=DATABASE() AND table_name=? AND constraint_name='PRIMARY' ORDER BY ordinal_position", table)
	if e != nil {
		return e
	}
	if len(keys) == 0 {
		return fmt.Errorf("missing primary key")
	}
	var myKeys, pgKeys []string
	for _, k := range keys {
		myKeys = append(myKeys, myIdent(k))
		pgKeys = append(pgKeys, pq.QuoteIdentifier(k))
	}
	stmt, e := dst.PrepareContext(ctx, pq.CopyIn(table, names...))
	if e != nil {
		return e
	}
	source, e := src.QueryContext(ctx, "SELECT "+strings.Join(my, ",")+" FROM "+m+" ORDER BY "+strings.Join(myKeys, ","))
	if e != nil {
		return e
	}
	scan := func(rows *sql.Rows, hash io.Writer, copyRows bool) (int, error) {
		defer rows.Close()
		enc := json.NewEncoder(hash)
		n := 0
		for rows.Next() {
			v := make([]sql.NullString, len(fields))
			args := make([]any, len(fields))
			for i := range v {
				args[i] = &v[i]
			}
			if err := rows.Scan(args...); err != nil {
				return n, err
			}
			for i, k := range fields {
				args[i] = nil
				if v[i].Valid {
					val := v[i].String
					if k.kind == "double precision" {
						f, err := strconv.ParseFloat(val, 64)
						if err != nil {
							return n, err
						}
						val = strconv.FormatFloat(f, 'g', -1, 64)
					}
					args[i] = val
				}
			}
			if err := enc.Encode(args); err != nil {
				return n, err
			}
			if copyRows {
				for i, f := range fields {
					if f.kind == "bytea" && v[i].Valid {
						args[i] = "\\x" + v[i].String
					}
				}
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return n, err
				}
			}
			n++
		}
		return n, rows.Err()
	}
	h1, h2 := sha256.New(), sha256.New()
	n, e := scan(source, h1, true)
	if e != nil {
		return e
	}
	if _, e = stmt.ExecContext(ctx); e != nil {
		return e
	}
	if e = stmt.Close(); e != nil {
		return e
	}
	target, e := dst.QueryContext(ctx, "SELECT "+strings.Join(pg, ",")+" FROM "+p+" ORDER BY "+strings.Join(pgKeys, ","))
	if e != nil {
		return e
	}
	n2, e := scan(target, h2, false)
	if e != nil {
		return e
	}
	if n != n2 || !slices.Equal(h1.Sum(nil), h2.Sum(nil)) {
		return fmt.Errorf("snapshot checksum mismatch (%d/%d rows)", n, n2)
	}
	for _, f := range fields {
		if f.identity {
			var seq string
			var next, high int64
			if e = dst.QueryRowContext(ctx, "SELECT pg_get_serial_sequence($1,$2)", table, f.name).Scan(&seq); e != nil {
				return e
			}
			if e = src.QueryRowContext(ctx, "SELECT COALESCE(AUTO_INCREMENT,1) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", table).Scan(&next); e != nil {
				return e
			}
			if e = dst.QueryRowContext(ctx, "SELECT COALESCE(MAX("+pq.QuoteIdentifier(f.name)+"),0)+1 FROM "+p).Scan(&high); e != nil {
				return e
			}
			if _, e = dst.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE %s RESTART WITH %d", seq, max(next, high))); e != nil {
				return e
			}
		}
	}
	log.Printf("verified %s: %d rows", table, n)
	return nil
}
func run(ctx context.Context) error {
	cfg, e := mysql.ParseDSN(os.Getenv("XZDP_SOURCE_MYSQL_DSN"))
	if e != nil {
		return e
	}
	if cfg.DBName == "" {
		return fmt.Errorf("missing source DSN")
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Params["information_schema_stats_expiry"] = "0"
	cfg.ParseTime = false
	src, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		return e
	}
	defer src.Close()
	dest := os.Getenv("XZDP_POSTGRES_DSN")
	if dest == "" {
		return fmt.Errorf("missing destination DSN")
	}
	pg, e := sql.Open("postgres", dest)
	if e != nil {
		return e
	}
	defer pg.Close()
	if e = migrate.Apply(ctx, pg); e != nil {
		return e
	}
	from, e := src.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return e
	}
	defer from.Rollback()
	to, e := pg.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer to.Rollback()
	if _, e = to.ExecContext(ctx, "SELECT pg_advisory_xact_lock(70751371)"); e != nil {
		return e
	}
	tables, e := list(ctx, from, "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_type='BASE TABLE' AND table_name<>'schema_migrations' ORDER BY table_name")
	if e != nil {
		return e
	}
	expected, e := list(ctx, to, "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('schema_migrations','spatial_ref_sys') ORDER BY tablename")
	if e != nil {
		return e
	}
	if !slices.Equal(tables, expected) {
		return fmt.Errorf("table mismatch: source %v, target %v", tables, expected)
	}
	var locks []string
	for _, table := range tables {
		locks = append(locks, pq.QuoteIdentifier(table))
	}
	if _, e = to.ExecContext(ctx, "LOCK TABLE "+strings.Join(locks, ",")+" IN ACCESS EXCLUSIVE MODE"); e != nil {
		return e
	}
	if _, e = to.ExecContext(ctx, "ALTER TABLE tb_shop DISABLE TRIGGER track_shop_geo"); e != nil {
		return e
	}
	for _, t := range tables {
		if e = transfer(ctx, from, to, t); e != nil {
			return fmt.Errorf("%s: %w", t, e)
		}
	}
	if _, e = to.ExecContext(ctx, "ALTER TABLE tb_shop ENABLE TRIGGER track_shop_geo"); e != nil {
		return e
	}
	return to.Commit()
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := run(ctx); e != nil {
		log.Fatal(e)
	}
}

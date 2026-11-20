package pgbench

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Copy from the populated MySQL source, then fingerprint both projections in PK order.
func migrate(t *testing.T, ctx context.Context, src, dst *sql.DB) {
	t.Helper()
	for _, table := range []struct {
		name   string
		fields string
	}{
		{"tb_shop", "id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time"},
		{"tb_user", "id,phone,password,nick_name,icon,create_time,update_time"},
		{"tb_blog", "id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time,like_version"},
		{"bench_slot", "id,remaining"},
	} {
		columns := strings.Split(table.fields, ",")
		tx, err := dst.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		copyStmt, err := tx.PrepareContext(ctx, pq.CopyIn(table.name, columns...))
		require.NoError(t, err)
		var fingerprints []string
		for _, db := range []*sql.DB{src, dst} {
			selects := append([]string(nil), columns...)
			for i, col := range columns {
				if db == dst && (col == "create_time" || col == "update_time") {
					selects[i] = "to_char(" + col + " AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
				} else if db == src && table.name == "tb_shop" && (col == "sold" || col == "comments" || col == "score") {
					selects[i] = "CAST(" + col + " AS UNSIGNED)"
				}
			}
			query := "SELECT " + strings.Join(selects, ",") + " FROM " + table.name + " ORDER BY id"
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err)
			hash := sha256.New()
			encoder := json.NewEncoder(hash)
			count := 0
			for rows.Next() {
				// All drivers return the numeric and text fields losslessly into strings.
				values := make([]sql.NullString, len(columns))
				args := make([]any, len(columns))
				for i := range values {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				for i, v := range values {
					if v.Valid {
						args[i] = v.String
					} else {
						args[i] = nil
					}
				}
				require.NoError(t, encoder.Encode(args))
				if db == src {
					_, err = copyStmt.ExecContext(ctx, args...)
					require.NoError(t, err)
				}
				count++
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			fingerprints = append(fingerprints, fmt.Sprintf("%d:%x", count, hash.Sum(nil)))
			if db == src {
				_, err = copyStmt.ExecContext(ctx)
				require.NoError(t, err)
				require.NoError(t, copyStmt.Close())
				require.NoError(t, tx.Commit())
			}
		}
		require.Equal(t, fingerprints[0], fingerprints[1], table.name)
		t.Logf("copy %s %s", table.name, fingerprints[0])
	}
}

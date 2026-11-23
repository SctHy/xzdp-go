package pgbench

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"xzdp/internal/discovery"
	schema "xzdp/internal/migrate"
)

// Use identical PostgreSQL tables, driver and durability settings for both versions.
func TestPostgresOptimizations(t *testing.T) {
	addr := os.Getenv("BENCH_PRODUCTION_PG")
	if addr == "" {
		t.Skip("use script/pgbench/production.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, e := sql.Open("pgx", "postgres://bench:bench-local@"+addr+"/bench?sslmode=disable")
	require.NoError(t, e)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	require.NoError(t, schema.Install(ctx, db, true))
	exec(t, db, "INSERT INTO tb_seckill_voucher(voucher_id,stock) VALUES(1,0)")
	exec(t, db, "ANALYZE")
	urls := []string{launch(t, db, "pg"), launch(t, db, "production")}
	var expected [2][35][]discovery.Hit
	for k := range expected {
		for i := range expected[k] {
			a, e := search(urls[0], path(i, k == 1))
			require.NoError(t, e)
			expected[k][i] = a
			b, e := search(urls[1], path(i, k == 1))
			require.NoError(t, e)
			require.NoError(t, checkHits(a, b))
		}
	}
	for _, kind := range []string{"near", "keyword", "order"} {
		for round := 0; round < 7; round++ {
			for j := range urls {
				v := (j + round) % 2
				if kind == "keyword" {
					// Only the optimized schema has the category/spatial index.
					if v == 0 {
						exec(t, db, "DROP INDEX IF EXISTS shop_category_location")
					} else {
						exec(t, db, "CREATE INDEX IF NOT EXISTS shop_category_location ON tb_shop USING gist(type_id,location)")
					}
					for i := 0; i < 35; i++ {
						_, err := search(urls[v], path(i, true))
						require.NoError(t, err)
					}
				}
				n := 480
				if round == 0 {
					n = 48
				}
				if kind == "order" {
					exec(t, db, "TRUNCATE trade_receipt,tb_voucher_order,trade_notice")
					exec(t, db, fmt.Sprintf("UPDATE tb_seckill_voucher SET stock=%d WHERE voucher_id=1", n))
				}
				r := load(t, n, func(i int) error {
					if kind == "order" {
						if v == 0 {
							return legacyOrder(ctx, db, int64(i+1))
						}
						_, e := db.ExecContext(ctx, "SELECT persist_voucher_order($1,$2,1)", i+1, i+1)
						return e
					}
					k := 0
					if kind == "keyword" {
						k = 1
					}
					hits, e := search(urls[v], path(i%35, k == 1))
					if e != nil {
						return e
					}
					return checkHits(expected[k][i%35], hits)
				})
				if kind == "order" {
					_, e := db.ExecContext(ctx, "SELECT persist_voucher_order(1,1,1)")
					require.NoError(t, e)
					_, e = db.ExecContext(ctx, "SELECT persist_voucher_order($1,$1,1)", n+1)
					require.ErrorContains(t, e, "database stock exhausted")
					var orders, receipts, notices, stock int
					e = db.QueryRow("SELECT (SELECT count(*) FROM tb_voucher_order),(SELECT count(*) FROM trade_receipt),(SELECT count(*) FROM trade_notice),stock FROM tb_seckill_voucher WHERE voucher_id=1").Scan(&orders, &receipts, &notices, &stock)
					require.NoError(t, e)
					require.Equal(t, []int{n, n, n, 0}, []int{orders, receipts, notices, stock})
				}
				if round > 0 {
					t.Logf("production %s %d %d %.3f %.3f %.1f", kind, v, round, r.p50, r.p95, r.qps)
				}
			}
		}
	}
	// Record representative plans after warming the fixture, outside timing.
	const origin = "ST_SetSRID(ST_MakePoint(120.149,30.316),4326)::geography"
	plan(t, target{name: "pg", db: db}, "knn", "SELECT * FROM (SELECT id,location <-> "+origin+" AS distance FROM tb_shop ORDER BY distance FETCH FIRST 10 ROWS WITH TIES) n WHERE distance<=1000 ORDER BY distance,id LIMIT 10")
	plan(t, target{name: "pg", db: db}, "category", "SELECT id,(location <-> "+origin+")*0.9999964257755911 AS meters FROM tb_shop WHERE type_id=1 AND strpos(lower(name),'茶餐厅')>0 AND ST_DWithin(location,"+origin+",2600,false) ORDER BY meters,id LIMIT 10")
	// Inspect the prepared statement actually used by the HTTP route, including
	// whether PostgreSQL selected a generic plan for the parameterized radius.
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	var prepared string
	var generic, custom int
	err = conn.QueryRowContext(ctx, "SELECT name,generic_plans,custom_plans FROM pg_prepared_statements WHERE statement LIKE 'SELECT id,name,type_id,(location <->%' AND statement LIKE '%strpos%' LIMIT 1").Scan(&prepared, &generic, &custom)
	if err == nil {
		t.Logf("keyword prepared plan generic=%d custom=%d", generic, custom)
		rows, e := conn.QueryContext(ctx, "EXPLAIN (ANALYZE,BUFFERS) EXECUTE "+pq.QuoteIdentifier(prepared)+"(120.149,30.316,0.9999964257755911,2600,0,1,'茶餐厅')")
		require.NoError(t, e)
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			t.Log("prepared keyword", line)
		}
		require.NoError(t, rows.Err())
		rows.Close()
	} else {
		require.ErrorIs(t, err, sql.ErrNoRows)
	}
	conn.Close()
	// More than one page of equal-distance shops exercises the KNN cutoff.
	exec(t, db, `INSERT INTO tb_shop(name,type_id,images,address,x,y,sold,comments,score)
	    SELECT 'Café_100%',42,'','',120,30,0,0,0 FROM generate_series(1,15)`)
	base := "?x=120.001&y=30"
	seed, err := search(urls[0], base)
	require.NoError(t, err)
	require.Len(t, seed, 10)
	for _, tail := range []string{"", "&page=2", "&typeId=42&q=Caf%C3%A9_100%25", fmt.Sprintf("&radiusMeters=%.8f", seed[0].Distance-.01), fmt.Sprintf("&radiusMeters=%.8f", seed[0].Distance+.01)} {
		a, err := search(urls[0], base+tail)
		require.NoError(t, err)
		b, err := search(urls[1], base+tail)
		require.NoError(t, err)
		require.NoError(t, checkHits(a, b))
	}

}
func legacyOrder(ctx context.Context, db *sql.DB, id int64) error {
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, e := tx.ExecContext(ctx, "INSERT INTO trade_receipt(order_id) VALUES ($1) ON CONFLICT DO NOTHING", id)
	if e != nil {
		return e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return e
	}
	if n == 0 {
		return tx.Commit()
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES($1,$2,1,1)", id, id); e != nil {
		return e
	}
	r, e = tx.ExecContext(ctx, "UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=1 AND stock>0")
	if e != nil {
		return e
	}
	n, e = r.RowsAffected()
	if e != nil {
		return e
	}
	if n == 0 {
		return errSoldOut
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO trade_notice(event_key,user_id,content) VALUES($1,$2,$3)", fmt.Sprintf("order:%d", id), id, fmt.Sprintf("优惠券订单 %d 已生成", id)); e != nil {
		return e
	}
	return tx.Commit()
}

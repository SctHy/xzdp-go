// Package pgbench compares an isolated MySQL source with a PostgreSQL shadow.
package pgbench

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"xzdp/internal/discovery"
)

const rankSQL = "SELECT b.id FROM tb_blog b JOIN tb_user u ON u.id=b.user_id ORDER BY b.liked DESC,b.id DESC LIMIT 100"
const pgRankSQL = "SELECT b.id FROM tb_blog b JOIN tb_user u ON u.id=b.user_id ORDER BY b.liked DESC NULLS LAST,b.id DESC LIMIT 100"
const seatSQL = "UPDATE bench_slot SET remaining=remaining-1 WHERE id=1 AND remaining>0"

type target struct {
	name     string
	db       *sql.DB
	endpoint string
}

func (x target) ranking() string {
	if x.name == "pg" {
		return pgRankSQL
	}
	return rankSQL
}

type result struct{ p50, p95, qps float64 }

func load(t *testing.T, n int, op func(int) error) result {
	t.Helper()
	latencies := make([]float64, n)
	var wg sync.WaitGroup
	var next atomic.Int64
	var once sync.Once
	var first error
	start := time.Now()
	for j := 0; j < 8; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				before := time.Now()
				err := op(i)
				latencies[i] = float64(time.Since(before)) / 1e6
				if err != nil {
					once.Do(func() { first = err })
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	require.NoError(t, first)
	sort.Float64s(latencies)
	return result{latencies[(n+1)/2-1], latencies[(95*n+99)/100-1], float64(n) / elapsed.Seconds()}
}
func ids(ctx context.Context, db *sql.DB, query string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		list = append(list, id)
	}
	return list, rows.Err()
}
func exec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.Exec(stmt)
	require.NoError(t, err)
}
func TestPostgresShadow(t *testing.T) {
	if os.Getenv("BENCH_MYSQL") == "" || os.Getenv("BENCH_PG") == "" {
		t.Skip("run script/pgbench/run.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	hlog.SetLevel(hlog.LevelError)
	my, err := sql.Open("mysql", "bench:bench-local@tcp("+os.Getenv("BENCH_MYSQL")+")/bench?charset=utf8mb4&timeout=3s&readTimeout=10s")
	require.NoError(t, err)
	pg, err := sql.Open("postgres", "postgres://bench:bench-local@"+os.Getenv("BENCH_PG")+"/bench?sslmode=disable&connect_timeout=3")
	require.NoError(t, err)
	for _, db := range []*sql.DB{my, pg} {
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(8)
		t.Cleanup(func() { db.Close() })
	}
	var myVersion, pgVersion string
	require.NoError(t, my.QueryRow("SELECT CONCAT(VERSION(), ' innodb_flush_log_at_trx_commit=', @@innodb_flush_log_at_trx_commit, ' sync_binlog=',@@sync_binlog)").Scan(&myVersion))
	require.NoError(t, pg.QueryRow("SELECT version() || ' PostGIS ' || postgis_lib_version() || ' fsync=' || current_setting('fsync') || ' synchronous_commit=' || current_setting('synchronous_commit')").Scan(&pgVersion))
	t.Logf("MySQL %s; PostgreSQL %s", myVersion, pgVersion)
	migrate(t, ctx, my, pg)
	exec(t, pg, "ANALYZE")
	exec(t, my, "ANALYZE TABLE tb_blog,tb_user,tb_shop")
	myURL, pgURL := launch(t, my, "mysql"), launch(t, pg, "pg")
	bounded := launch(t, my, "mysql-bounded")
	var want [2][35][]discovery.Hit
	for k := range want {
		for i := range want[k] {
			u := path(i, k == 1)
			want[k][i], err = search(myURL, u)
			require.NoError(t, err)
			if k == 0 {
				require.NotEmpty(t, want[k][i])
			} else if i%2 == 0 {
				require.NotEmpty(t, want[k][i])
			}
			hits, e := search(pgURL, u)
			require.NoError(t, e)
			require.NoError(t, checkHits(want[k][i], hits))
		}
	}
	for _, q := range []string{"?x=0&y=0&q=%25", "?x=120.13&y=30.25&radiusMeters=4500&page=2", "?x=-180&y=90&radiusMeters=1"} {
		a, e := search(myURL, q)
		require.NoError(t, e)
		b, e := search(pgURL, q)
		require.NoError(t, e)
		require.NoError(t, checkHits(a, b))
	}
	rank, e := ids(ctx, my, rankSQL)
	require.NoError(t, e)
	require.Len(t, rank, 5) // Four seed notes plus the NULL-rating test case.
	for stage := 0; stage < 2; stage++ {
		targets := []target{{"mysql", my, myURL}, {"pg", pg, pgURL}}
		if stage == 1 {
			exec(t, my, "ALTER TABLE tb_shop ADD COLUMN location POINT SRID 0")
			exec(t, my, "UPDATE tb_shop SET location=POINT(x,y)")
			exec(t, my, "ALTER TABLE tb_shop MODIFY location POINT NOT NULL SRID 0")
			exec(t, my, "CREATE SPATIAL INDEX bench_geo ON tb_shop(location)")
			exec(t, pg, "CREATE INDEX bench_geo ON tb_shop USING gist(location)")
			exec(t, my, "CREATE INDEX bench_rank ON tb_blog(liked DESC,id DESC)")
			exec(t, pg, "CREATE INDEX bench_rank ON tb_blog(liked DESC NULLS LAST,id DESC)")
			exec(t, pg, "ANALYZE")
			exec(t, my, "ANALYZE TABLE tb_blog,tb_shop")
			targets[0].endpoint = bounded
		}
		for _, kind := range []string{"near", "keyword", "rank", "detail", "seat", "order"} {
			if stage == 1 && (kind == "seat" || kind == "order" || kind == "detail") {
				continue
			}
			for round := 0; round < 5; round++ {
				for j := range targets {
					which := targets[(round+j)%2]
					count := 120
					if round == 0 {
						count = 24
					}
					r := measure(t, ctx, kind, which, count, rank, want)
					if round > 0 {
						t.Logf("sample %d %s %s %d %.3f %.3f %.1f", stage, kind, which.name, round, r.p50, r.p95, r.qps)
					}
				}
			}
		}
		for _, db := range targets {
			plan(t, db, "rank", db.ranking())
			if db.name == "pg" {
				plan(t, db, "near", pgNear, 120.149, 30.316, 0, "", 1000., 0, ratio)
			} else if stage == 1 {
				west, south, east, north := bounds(discovery.Query{X: 120.149, Y: 30.316, Radius: 1000})
				plan(t, db, "near", myBound, 120.149, 30.316, 0, 0, west, south, east, north, "", 1000., 0)
			}
		}
	}
	searchEdges(t, my, pg, myURL, bounded, pgURL)
}

func plan(t *testing.T, x target, name, stmt string, args ...any) {
	prefix := "EXPLAIN ANALYZE "
	if x.name == "pg" {
		prefix = "EXPLAIN (ANALYZE,BUFFERS) "
	}
	rows, err := x.db.Query(prefix+stmt, args...)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		t.Logf("plan %s %s %s", x.name, name, s)
	}
	require.NoError(t, rows.Err())
}

// Consume all payload columns just as the details read does after a rank hit.
func fetchPage(ctx context.Context, db *sql.DB, query string, ids []any) error {
	rows, err := db.QueryContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	var id int64
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	dest[0] = &id
	found := make(map[int64]bool, len(ids))
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		found[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range ids {
		if !found[item.(int64)] {
			return fmt.Errorf("missing blog %v", item)
		}
	}
	return nil
}

func detailQuery(which target, rank []int64) (string, []any) {
	pageArgs := make([]any, min(10, len(rank)))
	marks := make([]string, len(pageArgs))
	for i := range marks {
		pageArgs[i] = rank[i]
		marks[i] = "?"
		if which.name == "pg" {
			marks[i] = fmt.Sprintf("$%d", i+1)
		}
	}
	detailSQL := "SELECT b.*,u.nick_name AS author_name,u.icon AS author_icon FROM tb_blog b JOIN tb_user u ON u.id=b.user_id WHERE b.id IN (" + strings.Join(marks, ",") + ")"
	return detailSQL, pageArgs
}

func measure(t *testing.T, ctx context.Context, kind string, which target, count int, rank []int64, want [2][35][]discovery.Hit) result {
	t.Helper()
	if kind == "order" {
		resetOrders(t, which.db, count)
	}
	detailSQL, pageArgs := detailQuery(which, rank)
	var accepted atomic.Int64
	if kind == "seat" {
		exec(t, which.db, fmt.Sprintf("UPDATE bench_slot SET remaining=%d WHERE id=1", count*3/4))
	}
	r := load(t, count, func(i int) error {
		switch kind {
		case "detail":
			return fetchPage(ctx, which.db, detailSQL, pageArgs)
		case "order":
			return persist(ctx, which, int64(i+1))
		case "rank":
			sorted, err := ids(ctx, which.db, which.ranking())
			if err != nil {
				return err
			}
			if len(sorted) != len(rank) {
				return fmt.Errorf("rank length")
			}
			for j := range rank {
				if sorted[j] != rank[j] {
					return fmt.Errorf("rank changed")
				}
			}
		case "seat":
			rec, err := which.db.ExecContext(ctx, seatSQL)
			if err != nil {
				return err
			}
			n, err := rec.RowsAffected()
			if err != nil {
				return err
			}
			accepted.Add(n)
		default:
			k := 0
			if kind == "keyword" {
				k = 1
			}
			hits, err := search(which.endpoint, path(i%35, k == 1))
			if err != nil {
				return err
			}
			return checkHits(want[k][i%35], hits)
		}
		return nil
	})
	if kind == "seat" {
		require.EqualValues(t, count*3/4, accepted.Load())
		var left int
		require.NoError(t, which.db.QueryRow("SELECT remaining FROM bench_slot WHERE id=1").Scan(&left))
		require.Zero(t, left)
	}
	if kind == "order" {
		verifyOrders(t, ctx, which, count)
	}
	return r
}

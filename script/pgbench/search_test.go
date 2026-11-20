package pgbench

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/stretchr/testify/require"
	"xzdp/internal/discovery"
)

// The production SRID-0 MySQL query uses a sphere of radius 6370986 m.
// ST_DWithin is the indexable candidate filter. Rescale the PostGIS sphere to
// retain the source's radius, distance ordering and radius boundary.
const ratio = 6370986.0 / 6371008.7714150598
const pgNear = `WITH origin AS (SELECT ST_SetSRID(ST_MakePoint($1,$2),4326)::geography AS g)
 SELECT id,name,type_id,ST_Distance(location,g,false)*$7 AS meters FROM tb_shop,origin
 WHERE ($3::bigint=0 OR type_id=$3) AND strpos(lower(name),lower($4))>0
 AND ST_DWithin(location,g,$5/$7,false) ORDER BY meters,id LIMIT 10 OFFSET $6`
const myBound = `SELECT id,name,type_id,ST_Distance_Sphere(POINT(x,y),POINT(?,?)) AS meters FROM tb_shop
 WHERE (?=0 OR type_id=?) AND MBRIntersects(location,ST_MakeEnvelope(POINT(?,?),POINT(?,?))) AND LOCATE(?,name)>0 HAVING meters<=?
 ORDER BY meters,id LIMIT 10 OFFSET ?`

func bounds(q discovery.Query) (float64, float64, float64, float64) {
	angle := q.Radius / 6370986
	dy := angle * 180 / math.Pi
	south, north := math.Max(-90, q.Y-dy), math.Min(90, q.Y+dy)
	if south == -90 || north == 90 {
		return -180, south, 180, north
	}
	dx := math.Asin(math.Sin(angle)/math.Cos(q.Y*math.Pi/180)) * 180 / math.Pi
	if q.X-dx < -180 || q.X+dx > 180 {
		return -180, south, 180, north
	}
	return q.X - dx, south, q.X + dx, north
}

func launch(t *testing.T, db *sql.DB, engine string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	h := server.New(server.WithHostPorts(addr))
	switch engine {
	case "mysql":
		discovery.RegisterSearch(h, func(ctx context.Context, q discovery.Query) (*sql.Rows, error) {
			return db.QueryContext(ctx, `SELECT id,name,type_id,ST_Distance_Sphere(POINT(x,y),POINT(?,?)) AS meters FROM tb_shop
 WHERE (?=0 OR type_id=?) AND LOCATE(?,name)>0 HAVING meters<=? ORDER BY meters,id LIMIT 10 OFFSET ?`, q.X, q.Y, q.Type, q.Type, q.Q, q.Radius, (max(1, q.Page)-1)*10)
		})
	case "mysql-bounded":
		discovery.RegisterSearch(h, func(ctx context.Context, q discovery.Query) (*sql.Rows, error) {
			west, south, east, north := bounds(q)
			return db.QueryContext(ctx, myBound, q.X, q.Y, q.Type, q.Type, west, south, east, north, q.Q, q.Radius, (max(1, q.Page)-1)*10)
		})
	case "production":
		discovery.Register(h, db)
	case "pg":
		discovery.RegisterSearch(h, func(ctx context.Context, q discovery.Query) (*sql.Rows, error) {
			return db.QueryContext(ctx, pgNear, q.X, q.Y, q.Type, q.Q, q.Radius, (max(1, q.Page)-1)*10, ratio)
		})
	}
	done := make(chan error, 1)
	go func() { done <- h.Run() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		<-done
	})
	require.Eventually(t, func() bool {
		conn, e := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if e != nil {
			return false
		}
		conn.Close()
		return true
	}, 3*time.Second, 20*time.Millisecond)
	return "http://" + addr + "/shops/search"
}

var client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 12, DisableCompression: true}}

func search(endpoint, path string) ([]discovery.Hit, error) {
	res, err := client.Get(endpoint + path)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var reply struct {
		Success bool
		Data    []discovery.Hit
	}
	if err = json.Unmarshal(body, &reply); err != nil || res.StatusCode != 200 || !reply.Success {
		return nil, fmt.Errorf("search %s: %.200s", path, body)
	}
	return reply.Data, nil
}
func path(i int, keyword bool) string {
	x := 121.39 + float64(i%7)*.002
	y := 31.19 + float64(i%5)*.003
	base := fmt.Sprintf("?x=%f&y=%f", x, y)
	if keyword {
		return base + fmt.Sprintf("&radiusMeters=2600&typeId=2&q=%s&page=%d", url.QueryEscape("咖啡馆"), 1+i%2)
	}
	return base + "&radiusMeters=1000"
}
func checkHits(a, b []discovery.Hit) error {
	if len(a) != len(b) {
		return fmt.Errorf("length %d != %d", len(a), len(b))
	}
	for i, x := range a {
		y := b[i]
		if x.ID != y.ID || x.Name != y.Name || x.Type != y.Type || math.Abs(x.Distance-y.Distance) > .002 {
			return fmt.Errorf("different hits: %+v %+v", x, y)
		}
	}
	return nil
}

// Exercise radius boundaries, collation examples and ID tie-breaking in a
// populated area, after timing so these fixtures never affect a measured run.
func searchEdges(t *testing.T, my, pg *sql.DB, endpoints ...string) {
	t.Helper()
	exec(t, my, `INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score,location)
	VALUES (990001,'Café_100%门店',999,'','',120,30,0,0,0,POINT(120,30)),(990002,'CAFÉ_100%门店',999,'','',120,30,0,0,0,POINT(120,30))`)
	exec(t, pg, `INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score)
	VALUES (990001,'Café_100%门店',999,'','',120,30,0,0,0),(990002,'CAFÉ_100%门店',999,'','',120,30,0,0,0)`)
	prefix := "?x=120.001&y=30&typeId=999&q="
	for _, endpoint := range endpoints {
		plain, err := search(endpoint, prefix+url.QueryEscape("cafe_100%"))
		require.NoError(t, err)
		require.Empty(t, plain)
	}
	base := prefix + url.QueryEscape("café_100%")
	a, err := search(endpoints[0], base)
	require.NoError(t, err)
	require.Len(t, a, 2)
	require.EqualValues(t, 990001, a[0].ID)
	require.EqualValues(t, 990002, a[1].ID)
	for _, url := range endpoints {
		for _, delta := range []float64{-.01, .01} {
			hits, err := search(url, base+fmt.Sprintf("&radiusMeters=%.9f", a[0].Distance+delta))
			require.NoError(t, err)
			if delta < 0 {
				require.Empty(t, hits)
			} else {
				require.NoError(t, checkHits(a, hits))
			}
		}
	}
}

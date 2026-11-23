package shop

import (
	"context"
	"database/sql"
	"math"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	api "xzdp/internal/httpapi/endpoint"
)

type Hit struct {
	ID       int64   `json:"id"`
	Name     string  `json:"name"`
	Type     int64   `json:"typeId"`
	Distance float64 `json:"distanceMeters"`
}

type Query struct {
	Q      string  `query:"q"`
	Type   int64   `query:"typeId"`
	X      float64 `query:"x"`
	Y      float64 `query:"y"`
	Radius float64 `query:"radiusMeters"`
	Page   int     `query:"page"`
}

func Register(h *server.Hertz, db *sql.DB) {
	RegisterSearch(h, func(ctx context.Context, q Query) (*sql.Rows, error) { return Search(ctx, db, q) })
}
func Search(ctx context.Context, db *sql.DB, q Query) (*sql.Rows, error) {
	// Preserve the radius of the previous SRID-0 distance calculation.
	const scale = 6370986.0 / 6371008.7714150598
	const origin = "ST_SetSRID(ST_MakePoint($1,$2),4326)::geography"
	if q.Type == 0 && q.Q == "" && q.Page <= 1 {
		// Fetch boundary ties so the public ID tie-break remains deterministic.
		return db.QueryContext(ctx, `SELECT id,name,type_id,distance*$3 AS meters
				FROM (SELECT id,name,type_id,location <-> `+origin+` AS distance FROM tb_shop
				      ORDER BY distance FETCH FIRST 10 ROWS WITH TIES) nearest
				WHERE distance <= $4 ORDER BY distance,id LIMIT 10`, q.X, q.Y, scale, q.Radius/scale)
	}
	query := "SELECT id,name,type_id,(location <-> " + origin + ")*$3 AS meters FROM tb_shop WHERE ST_DWithin(location," + origin + ",$4,false)"
	args := []any{q.X, q.Y, scale, q.Radius / scale, (max(1, q.Page) - 1) * 10}
	if q.Type != 0 {
		args = append(args, q.Type)
		query += " AND type_id=$6"
	}
	if q.Q != "" {
		args = append(args, q.Q)
		query += " AND strpos(lower(name), lower($" + strconv.Itoa(len(args)) + "))>0"
	}
	order := "location <-> " + origin + ",id"
	if q.Type != 0 || q.Q != "" {
		// Sort the qualifying rows. A distance-first index walk can visit many
		// unrelated shops before it encounters enough keyword matches.
		order = "meters,id"
	}
	return db.QueryContext(ctx, query+" ORDER BY "+order+" LIMIT 10 OFFSET $5", args...)
}

func RegisterSearch(h *server.Hertz, search func(context.Context, Query) (*sql.Rows, error)) {
	h.GET("/shops/search", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		var q Query
		if err := c.BindQuery(&q); err != nil || c.Query("x") == "" || c.Query("y") == "" || math.IsNaN(q.X+q.Y+q.Radius) || math.Abs(q.X) > 180 || math.Abs(q.Y) > 90 || q.Radius < 0 || q.Radius > 20000000 || q.Page < 0 || q.Page > 1000000 || q.Type < 0 {
			return nil, api.Problem("搜索参数无效")
		}
		if q.X == -180 {
			q.X = 180
		}
		if q.Radius == 0 {
			q.Radius = 5000
		}
		rows, err := search(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		result := []Hit{}
		for rows.Next() {
			var v Hit
			if err := rows.Scan(&v.ID, &v.Name, &v.Type, &v.Distance); err != nil {
				return nil, err
			}
			result = append(result, v)
		}
		return result, rows.Err()
	}))
}

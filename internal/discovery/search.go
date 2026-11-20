package discovery

import (
	"context"
	"database/sql"
	"math"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"xzdp/internal/api"
)

type Hit struct {
	ID       int64   `json:"id"`
	Name     string  `json:"name"`
	Type     int64   `json:"typeId"`
	Distance float64 `json:"distanceMeters"`
}

func Register(h *server.Hertz, db *sql.DB) {
	h.GET("/shops/search", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		var q struct {
			Q      string  `query:"q"`
			Type   int64   `query:"typeId"`
			X      float64 `query:"x"`
			Y      float64 `query:"y"`
			Radius float64 `query:"radiusMeters"`
			Page   int     `query:"page"`
		}
		if err := c.BindQuery(&q); err != nil || c.Query("x") == "" || c.Query("y") == "" || math.IsNaN(q.X+q.Y+q.Radius) || math.Abs(q.X) > 180 || math.Abs(q.Y) > 90 || q.Radius < 0 || q.Radius > 20000000 || q.Page < 0 || q.Page > 1000000 || q.Type < 0 {
			return nil, api.Problem("搜索参数无效")
		}
		if q.X == -180 {
			q.X = 180
		}
		if q.Radius == 0 {
			q.Radius = 5000
		}
		rows, err := db.QueryContext(ctx, `SELECT id,name,type_id,ST_Distance_Sphere(POINT(x,y),POINT(?,?)) AS meters FROM tb_shop
 WHERE (?=0 OR type_id=?) AND LOCATE(?,name)>0 HAVING meters<=? ORDER BY meters,id LIMIT 10 OFFSET ?`, q.X, q.Y, q.Type, q.Type, q.Q, q.Radius, (max(1, q.Page)-1)*10)
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

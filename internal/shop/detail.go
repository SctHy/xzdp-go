package shop

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"xzdp/internal/cache"
	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/store"
)

func Detail(ctx context.Context, db *sql.DB, c *cache.Cache, id int64) (json.RawMessage, error) {
	b, e := c.Get(ctx, strconv.FormatInt(id, 10), func(ctx context.Context) (json.RawMessage, error) {
		var b []byte
		e := db.QueryRowContext(ctx, "SELECT row_to_json(s) FROM (SELECT id,name,address,type_id,open_hours,score,x,y FROM tb_shop WHERE id=$1) s", id).Scan(&b)
		if errors.Is(e, sql.ErrNoRows) {
			return nil, nil
		}
		return b, e
	})
	if len(b) == 0 && e == nil {
		return nil, sql.ErrNoRows
	}
	return b, e
}
func RegisterDetail(h *server.Hertz, db *sql.DB, cache *cache.Cache) {
	h.GET("/shops/:id", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		id, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		return Detail(ctx, db, cache, id)
	}))
	h.GET("/categories", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		return store.List(ctx, db, "SELECT id,name FROM tb_shop_type ORDER BY sort,id")
	}))
}

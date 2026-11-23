package social

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	api "xzdp/internal/httpapi/endpoint"
	"xzdp/internal/store"
)

type post struct {
	Title, Content string
	ShopID         int64
}

func Register(h *server.Hertz, db *sql.DB) {
	notes := func(feed bool) app.HandlerFunc {
		return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
			q := "SELECT b.id,b.user_id,b.shop_id,b.title,b.content,b.liked,coalesce(u.nick_name,'访客') author FROM tb_blog b LEFT JOIN tb_user u ON u.id=b.user_id "
			if feed {
				return store.List(ctx, db, q+"WHERE EXISTS(SELECT FROM tb_follow f WHERE f.follow_user_id=b.user_id AND f.user_id=$1) ORDER BY b.id DESC LIMIT 100", api.UserID(ctx))
			}
			return store.List(ctx, db, q+"ORDER BY b.liked DESC NULLS LAST,b.id DESC LIMIT 100")
		})
	}
	h.GET("/notes", notes(false))
	h.GET("/feed", notes(true))
	h.POST("/notes", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		var p post
		if c.BindJSON(&p) != nil || strings.TrimSpace(p.Title) == "" || strings.TrimSpace(p.Content) == "" || len([]rune(p.Title)) > 255 || len([]rune(p.Content)) > 2048 {
			return nil, api.Problem("请填写标题和正文")
		}
		var id int64
		e := db.QueryRowContext(ctx, "INSERT INTO tb_blog(shop_id,user_id,title,images,content) SELECT id,$1,$2,'',$3 FROM tb_shop WHERE id=$4 RETURNING id", api.UserID(ctx), p.Title, p.Content, p.ShopID).Scan(&id)
		return id, e
	}))
	h.POST("/notes/:id/like", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		id, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback()
		var n int64
		if e = tx.QueryRowContext(ctx, "SELECT liked FROM tb_blog WHERE id=$1 FOR UPDATE", id).Scan(&n); e != nil {
			return nil, e
		}
		u := api.UserID(ctx)
		r, e := tx.ExecContext(ctx, "DELETE FROM note_like WHERE note_id=$1 AND user_id=$2", id, u)
		if e != nil {
			return nil, e
		}
		deleted, e := r.RowsAffected()
		if e != nil {
			return nil, e
		}
		if deleted == 0 {
			if _, e = tx.ExecContext(ctx, "INSERT INTO note_like VALUES($1,$2)", id, u); e != nil {
				return nil, e
			}
			n++
		} else {
			n--
		}
		if _, e = tx.ExecContext(ctx, "UPDATE tb_blog SET liked=$1 WHERE id=$2", n, id); e != nil {
			return nil, e
		}
		return n, tx.Commit()
	}))
	h.GET("/notes/:id/comments", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		id, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		return store.List(ctx, db, "SELECT id,user_id,content FROM tb_blog_comments WHERE blog_id=$1 ORDER BY id DESC LIMIT 100", id)
	}))
	h.POST("/notes/:id/comments", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		id, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		var p post
		if c.BindJSON(&p) != nil || strings.TrimSpace(p.Content) == "" || len([]rune(p.Content)) > 255 {
			return nil, api.Problem("评论长度应为 1–255 字")
		}
		var out int64
		e = db.QueryRowContext(ctx, "INSERT INTO tb_blog_comments(blog_id,user_id,content,parent_id,answer_id) SELECT id,$2,$3,0,0 FROM tb_blog WHERE id=$1 RETURNING id", id, api.UserID(ctx), p.Content).Scan(&out)
		return out, e
	}))
	follow := func(remove bool) app.HandlerFunc {
		return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
			id, e := api.ID(c)
			if e != nil {
				return nil, e
			}
			u := api.UserID(ctx)
			if id == u {
				return nil, api.Problem("不能关注自己")
			}
			q := "INSERT INTO tb_follow(user_id,follow_user_id) SELECT $1,id FROM tb_user WHERE id=$2 ON CONFLICT DO NOTHING"
			if remove {
				q = "DELETE FROM tb_follow WHERE user_id=$1 AND follow_user_id=$2"
			}
			_, e = db.ExecContext(ctx, q, u, id)
			return !remove, e
		})
	}
	h.POST("/following/:id", follow(false))
	h.DELETE("/following/:id", follow(true))
	h.GET("/following/:id/common", api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		id, e := api.ID(c)
		if e != nil {
			return nil, e
		}
		return store.List(ctx, db, "SELECT follow_user_id id FROM tb_follow WHERE user_id=$1 INTERSECT SELECT follow_user_id FROM tb_follow WHERE user_id=$2 ORDER BY id LIMIT 100", id, api.UserID(ctx))
	}))
}

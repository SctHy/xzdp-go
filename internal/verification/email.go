// Package verification owns purpose-bound email challenges and account bindings.
package verification

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/mail"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"

	"xzdp/biz/utils"
	"xzdp/internal/api"
)

type Service struct {
	DB     *sql.DB
	Redis  *redis.Client
	Prefix string
	SMTP   SMTP
}
type challenge struct {
	Email   string `json:"email"`
	Code    string `json:"code"`
	Purpose string `json:"purpose"`
}

func parse(c *app.RequestContext) (challenge, error) {
	var v challenge
	if c.BindJSON(&v) != nil {
		return v, api.Problem("JSON 格式错误")
	}
	v.Email = strings.ToLower(strings.TrimSpace(v.Email))
	a, e := mail.ParseAddress(v.Email)
	if e != nil || a.Address != v.Email || len(v.Email) > 254 {
		return v, api.Problem("邮箱格式错误")
	}
	return v, nil
}
func digest(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
func (s Service) key(ctx context.Context, v challenge) (string, error) {
	purpose := v.Purpose
	switch purpose {
	case "login":
	case "bind":
		u := utils.GetUser(ctx)
		if u == nil {
			return "", api.Problem("请先登录")
		}
		purpose = fmt.Sprintf("bind:%d", u.ID)
	default:
		return "", api.Problem("用途须为 login 或 bind")
	}
	return s.Prefix + purpose + ":" + digest(v.Email), nil
}

var issue = redis.NewScript(`
if redis.call('EXISTS',KEYS[2])==1 or tonumber(redis.call('GET',KEYS[3]) or '0')>=20 then return 0 end
redis.call('SET',KEYS[2],1,'EX',60)
if redis.call('INCR',KEYS[3])==1 then redis.call('EXPIRE',KEYS[3],600) end
redis.call('HSET',KEYS[1],'code',ARGV[1],'tries',0);redis.call('EXPIRE',KEYS[1],300);return 1`)
var verify = redis.NewScript(`
local code=redis.call('HGET',KEYS[1],'code')
if not code then return 0 end
if code==ARGV[1] then redis.call('DEL',KEYS[1]);return 1 end
if redis.call('HINCRBY',KEYS[1],'tries',1)>=5 then redis.call('DEL',KEYS[1]) end
return 0`)

func (s Service) send(ctx context.Context, c *app.RequestContext) (any, error) {
	v, e := parse(c)
	if e != nil {
		return nil, e
	}
	k, e := s.key(ctx, v)
	if e != nil {
		return nil, e
	}
	if s.SMTP.Address == "" {
		return nil, api.Problem("邮件服务未配置")
	}
	n, e := rand.Int(rand.Reader, big.NewInt(1000000))
	if e != nil {
		return nil, e
	}
	code := fmt.Sprintf("%06d", n)
	host, _, e := net.SplitHostPort(c.RemoteAddr().String())
	if e != nil {
		return nil, e
	}
	nIssued, e := issue.Run(ctx, s.Redis, []string{k, s.Prefix + "cooldown:" + digest(v.Email), s.Prefix + "ip:" + digest(host)}, code).Int()
	if e != nil {
		return nil, e
	}
	if nIssued == 0 {
		return nil, api.Problem("发送过于频繁")
	}
	if e = s.SMTP.Send(ctx, v.Email, code); e != nil {
		return nil, e
	}
	return "sent", nil
}
func (s Service) complete(purpose string) app.HandlerFunc {
	return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
		v, e := parse(c)
		if e != nil {
			return nil, e
		}
		v.Purpose = purpose
		k, e := s.key(ctx, v)
		if e != nil {
			return nil, e
		}
		ok, e := verify.Run(ctx, s.Redis, []string{k}, v.Code).Int()
		if e != nil {
			return nil, e
		}
		if ok == 0 {
			return nil, api.Problem("验证码无效或已过期")
		}
		if purpose == "bind" {
			id := utils.GetUser(ctx).ID
			_, e = s.DB.ExecContext(ctx, "INSERT INTO user_email VALUES(?,?)", id, v.Email)
			if duplicate(e) {
				var bound string
				if err := s.DB.QueryRowContext(ctx, "SELECT email FROM user_email WHERE user_id=?", id).Scan(&bound); err == nil && bound == v.Email {
					return "bound", nil
				}
				return nil, api.Problem("邮箱或账号已有绑定")
			}
			return "bound", e
		}
		id, e := s.account(ctx, v.Email)
		if e != nil {
			return nil, e
		}
		var nick, icon string
		e = s.DB.QueryRowContext(ctx, "SELECT COALESCE(nick_name,''),COALESCE(icon,'') FROM tb_user WHERE id=?", id).Scan(&nick, &icon)
		if e != nil {
			return nil, e
		}
		return utils.CreateSession(ctx, s.Redis, id, nick, icon)
	})
}
func duplicate(e error) bool { var m *mysql.MySQLError; return errors.As(e, &m) && m.Number == 1062 }
func (s Service) account(ctx context.Context, email string) (int64, error) {
	lookup := func() (int64, error) {
		var id int64
		e := s.DB.QueryRowContext(ctx, "SELECT user_id FROM user_email WHERE email=?", email).Scan(&id)
		return id, e
	}
	id, e := lookup()
	if !errors.Is(e, sql.ErrNoRows) {
		return id, e
	}
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	r, e := tx.ExecContext(ctx, "INSERT INTO tb_user(nick_name) VALUES(?)", "user_"+utils.RandomString(10))
	if e != nil {
		return 0, e
	}
	id, e = r.LastInsertId()
	if e != nil {
		return 0, e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO user_email VALUES(?,?)", id, email)
	if duplicate(e) {
		tx.Rollback()
		return lookup()
	}
	if e != nil {
		return 0, e
	}
	return id, tx.Commit()
}
func (s Service) Register(h *server.Hertz) {
	h.POST("/user/email/code", api.Endpoint(s.send))
	h.POST("/user/email/login", s.complete("login"))
	h.POST("/user/email/bind", s.complete("bind"))
}

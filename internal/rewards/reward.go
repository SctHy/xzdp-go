package rewards

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-redis/redis/v8"

	"xzdp/biz/pkg/constants"
	"xzdp/biz/utils"
	"xzdp/internal/api"
)

type Progress struct {
	Month    string `json:"month"`
	Days     int    `json:"days"`
	Required int    `json:"daysRequired"`
	Voucher  int64  `json:"voucherId"`
	Claimed  bool   `json:"claimed"`
}

func Register(h *server.Hertz, db *sql.DB, r *redis.Client) {
	s := service{db, r}
	handler := func(claim bool) app.HandlerFunc {
		return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
			return s.progress(ctx, utils.GetUser(ctx).ID, time.Now(), claim)
		})
	}
	h.GET("/user/sign/reward", handler(false))
	h.POST("/user/sign/reward/claim", handler(true))
}

type service struct {
	db    *sql.DB
	redis *redis.Client
}

func (s service) progress(ctx context.Context, u int64, now time.Time, claim bool) (*Progress, error) {
	month := now.Format("2006-01") + "-01"
	p := Progress{Month: month}
	// Redis numbers bits from the most significant bit of each byte.
	lastByte := int64((now.Day() - 1) / 8)
	bitmap, e := s.redis.GetRange(ctx, constants.USER_SIGN_KEY+fmt.Sprint(u)+now.Format(":200601"), 0, lastByte).Bytes()
	if e != nil {
		return nil, e
	}
	for i, b := range bitmap {
		if int64(i) == lastByte {
			b &= byte(0xff << (7 - (now.Day()-1)%8))
		}
		p.Days += bits.OnesCount8(b)
	}
	tx, e := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	// Ordinary voucher 1 is the seed reward; a monthly rule can override it.
	e = tx.QueryRowContext(ctx, `SELECT COALESCE(r.days_required,7),v.id FROM (SELECT 1) AS seed
 LEFT JOIN sign_reward_rule r ON r.month_start=$1 JOIN tb_voucher v ON v.id=COALESCE(r.voucher_id,1) AND v.type=0 AND v.status=1 FOR SHARE OF v`, month).Scan(&p.Required, &p.Voucher)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if claim && p.Voucher != 0 && p.Days >= p.Required {
		_, e = tx.ExecContext(ctx, "INSERT INTO sign_reward_grant VALUES($1,$2,$3,statement_timestamp()) ON CONFLICT DO NOTHING", u, month, p.Voucher)
		if e != nil {
			return nil, e
		}
	}
	var granted int64
	e = tx.QueryRowContext(ctx, "SELECT voucher_id FROM sign_reward_grant WHERE user_id=$1 AND month_start=$2", u, month).Scan(&granted)
	if e == nil {
		p.Claimed = true
		p.Voucher = granted
	} else if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if claim && !p.Claimed && p.Voucher == 0 {
		return nil, api.Problem("本月尚未配置有效普通券")
	}
	if claim && !p.Claimed {
		return nil, api.Problem("累计签到天数不足")
	}
	return &p, tx.Commit()
}

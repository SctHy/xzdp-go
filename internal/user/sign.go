package user

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/redis/go-redis/v9"

	api "xzdp/internal/httpapi/endpoint"
)

type Progress struct {
	Month    string `json:"month"`
	Days     int    `json:"days"`
	Required int    `json:"daysRequired"`
	Voucher  int64  `json:"voucherId"`
	Streak   int    `json:"streak"`
	Signed   bool   `json:"signed"`
	Claimed  bool   `json:"claimed"`
}

func RegisterSign(h *server.Hertz, db *sql.DB, r *redis.Client, prefix string) {
	s := service{db, r, prefix}
	handler := func(sign, claim bool) app.HandlerFunc {
		return api.Endpoint(func(ctx context.Context, c *app.RequestContext) (any, error) {
			return s.progress(ctx, api.UserID(ctx), time.Now().In(time.FixedZone("UTC+8", 8*3600)), sign, claim)
		})
	}
	h.POST("/user/sign", handler(true, true))
	h.GET("/user/sign/reward", handler(false, false))
	h.POST("/user/sign/reward/claim", handler(false, true))
}

type service struct {
	db     *sql.DB
	redis  *redis.Client
	prefix string
}

func (s service) progress(ctx context.Context, u int64, now time.Time, sign, claim bool) (*Progress, error) {
	month := now.Format("2006-01") + "-01"
	p := Progress{Month: month}
	// Redis numbers bits from the most significant bit of each byte.
	lastByte := int64((now.Day() - 1) / 8)
	key := s.prefix + "sign:" + fmt.Sprint(u) + now.Format(":200601")
	if sign {
		if err := s.redis.SetBit(ctx, key, int64(now.Day()-1), 1).Err(); err != nil {
			return nil, err
		}
	}
	bitmap, e := s.redis.GetRange(ctx, key, 0, lastByte).Bytes()
	if e != nil {
		return nil, e
	}
	p.Days, p.Streak, p.Signed = signCounts(bitmap, now.Day())
	tx, e := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	// Ordinary voucher 1 is the seed reward; a monthly rule can override it.
	e = tx.QueryRowContext(ctx, `SELECT COALESCE(r.days_required,extract(day from ($1::date+interval '1 month - 1 day'))::int),v.id FROM (SELECT 1) AS seed
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

	return &p, tx.Commit()
}

// signCounts reads this month's Redis bitmap through today (one-based).
func signCounts(bitmap []byte, today int) (days, streak int, signed bool) {
	for d := 0; d < today; d++ {
		marked := d/8 < len(bitmap) && bitmap[d/8]&(0x80>>(d%8)) != 0
		if marked {
			days++
			streak++
		} else if d+1 < today {
			streak = 0
		}
		if d+1 == today {
			signed = marked
		}
	}
	return
}

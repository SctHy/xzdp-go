package booking

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-sql-driver/mysql"

	"xzdp/biz/utils"
	"xzdp/internal/api"
)

type service struct{ db *sql.DB }
type Slot struct {
	ID        int64     `json:"id"`
	Start     time.Time `json:"startsAt"`
	End       time.Time `json:"endsAt"`
	Capacity  int       `json:"capacity"`
	Remaining int       `json:"remaining"`
}
type Booking struct {
	ID        int64 `json:"id"`
	Slot      int64 `json:"slotId"`
	Party     int   `json:"partySize"`
	Cancelled bool  `json:"cancelled"`
}

func Register(h *server.Hertz, db *sql.DB) {
	s := service{db}
	h.GET("/shops/:id/slots", api.Endpoint(s.slots))
	h.POST("/shops/:id/slots", api.Endpoint(s.createSlot))
	h.POST("/slots/:id/bookings", api.Endpoint(s.reserve))
	h.GET("/bookings/:id", api.Endpoint(s.get))
	h.POST("/bookings/:id/cancel", api.Endpoint(s.cancel))
}
func (s service) slots(ctx context.Context, c *app.RequestContext) (any, error) {
	id, e := api.ID(c)
	if e != nil {
		return nil, e
	}
	rows, e := s.db.QueryContext(ctx, "SELECT id,starts_at,ends_at,capacity,remaining FROM booking_slot WHERE shop_id=? AND starts_at>UTC_TIMESTAMP() ORDER BY starts_at", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := []Slot{}
	for rows.Next() {
		var v Slot
		if e = rows.Scan(&v.ID, &v.Start, &v.End, &v.Capacity, &v.Remaining); e != nil {
			return nil, e
		}
		result = append(result, v)
	}
	return result, rows.Err()
}
func (s service) createSlot(ctx context.Context, c *app.RequestContext) (any, error) {
	id, e := api.ID(c)
	if e != nil {
		return nil, e
	}
	var v Slot
	if c.BindJSON(&v) != nil || !v.Start.After(time.Now()) || !v.End.After(v.Start) || v.Capacity < 1 || v.Capacity > 100000 {
		return nil, api.Problem("时段或容量无效")
	}
	r, e := s.db.ExecContext(ctx, `INSERT IGNORE INTO booking_slot(shop_id,starts_at,ends_at,capacity,remaining) SELECT st.shop_id,?,?,?,? FROM shop_staff st JOIN tb_shop shop ON shop.id=st.shop_id WHERE st.shop_id=? AND st.user_id=?`, v.Start.UTC(), v.End.UTC(), v.Capacity, v.Capacity, id, utils.GetUser(ctx).ID)
	if e != nil {
		return nil, e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return nil, e
	}
	if n != 1 {
		return nil, api.Problem("门店权限不足或时段已存在")
	}
	return r.LastInsertId()
}

// Acquire the slot first so UTC_TIMESTAMP() in the following conditional write
// reflects the time after any wait for a competing reservation.
func lockSlot(ctx context.Context, tx *sql.Tx, id int64) error {
	var locked int64
	return tx.QueryRowContext(ctx, "SELECT id FROM booking_slot WHERE id=? FOR UPDATE", id).Scan(&locked)
}

func (s service) reserve(ctx context.Context, c *app.RequestContext) (any, error) {
	slot, e := api.ID(c)
	if e != nil {
		return nil, e
	}
	var input Booking
	key := string(c.GetHeader("Idempotency-Key"))
	if c.BindJSON(&input) != nil || input.Party < 1 || input.Party > 100000 || len(key) < 1 || len(key) > 64 {
		return nil, api.Problem("请提供有效人数与幂等键")
	}
	user := utils.GetUser(ctx).ID
	tx, e := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	// Serialize capacity changes per slot. A full slot never creates a booking,
	// so a group of retries can all fail without contending on a provisional row.
	if e = lockSlot(ctx, tx, slot); e != nil {
		return nil, e
	}
	var b Booking
	e = tx.QueryRowContext(ctx, "SELECT id,slot_id,party_size,cancelled_at IS NOT NULL FROM booking WHERE user_id=? AND request_key=?", user, key).Scan(&b.ID, &b.Slot, &b.Party, &b.Cancelled)
	if e == nil {
		if b.Party != input.Party || b.Slot != slot {
			return nil, api.Problem("幂等键已用于另一份预约")
		}
		return b, tx.Commit()
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	r, e := tx.ExecContext(ctx, "UPDATE booking_slot SET remaining=remaining-? WHERE id=? AND starts_at>UTC_TIMESTAMP() AND remaining>=?", input.Party, slot, input.Party)
	if e != nil {
		return nil, e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return nil, e
	}
	if n != 1 {
		return nil, api.Problem("时段不可用或余位不足")
	}
	r, e = tx.ExecContext(ctx, "INSERT INTO booking(slot_id,user_id,party_size,request_key) VALUES(?,?,?,?)", slot, user, input.Party, key)
	var mysqlErr *mysql.MySQLError
	if errors.As(e, &mysqlErr) && mysqlErr.Number == 1062 {
		// A concurrent request used this key for a different slot.
		return nil, api.Problem("幂等键已用于另一份预约")
	}
	if e != nil {
		return nil, e
	}
	b.Slot, b.Party = slot, input.Party
	b.ID, e = r.LastInsertId()
	if e != nil {
		return nil, e
	}
	return b, tx.Commit()
}
func (s service) get(ctx context.Context, c *app.RequestContext) (any, error) {
	id, e := api.ID(c)
	if e != nil {
		return nil, e
	}
	var b Booking
	e = s.db.QueryRowContext(ctx, "SELECT id,slot_id,party_size,cancelled_at IS NOT NULL FROM booking WHERE id=? AND user_id=?", id, utils.GetUser(ctx).ID).Scan(&b.ID, &b.Slot, &b.Party, &b.Cancelled)
	return b, e
}
func (s service) cancel(ctx context.Context, c *app.RequestContext) (any, error) {
	id, e := api.ID(c)
	if e != nil {
		return nil, e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var b Booking
	e = tx.QueryRowContext(ctx, "SELECT id,slot_id,party_size,cancelled_at IS NOT NULL FROM booking WHERE id=? AND user_id=? FOR UPDATE", id, utils.GetUser(ctx).ID).Scan(&b.ID, &b.Slot, &b.Party, &b.Cancelled)
	if e != nil {
		return nil, e
	}
	if !b.Cancelled {
		if e = lockSlot(ctx, tx, b.Slot); e != nil {
			return nil, e
		}
		r, e := tx.ExecContext(ctx, "UPDATE booking_slot SET remaining=remaining+? WHERE id=? AND starts_at>UTC_TIMESTAMP()", b.Party, b.Slot)
		if e != nil {
			return nil, e
		}
		n, e := r.RowsAffected()
		if e != nil {
			return nil, e
		}
		if n != 1 {
			return nil, api.Problem("时段已开始")
		}
		if _, e = tx.ExecContext(ctx, "UPDATE booking SET cancelled_at=UTC_TIMESTAMP() WHERE id=?", id); e != nil {
			return nil, e
		}
		b.Cancelled = true
	}
	return b, tx.Commit()
}

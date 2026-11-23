package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/redis/go-redis/v9"

	"xzdp/internal/httpapi"
	"xzdp/internal/store"
	"xzdp/internal/trade"
)

type browser struct {
	h     *server.Hertz
	token string
}

func (b browser) request(method, path, body string, code int) (json.RawMessage, error) {
	r := ut.PerformRequest(b.h.Engine, method, path, &ut.Body{Body: strings.NewReader(body), Len: len(body)}, ut.Header{Key: "Content-Type", Value: "application/json"}, ut.Header{Key: "Authorization", Value: b.token}, ut.Header{Key: "Idempotency-Key", Value: "same-request"})
	if r.Code != code {
		return nil, fmt.Errorf("%s %s: got %d, want %d: %s", method, path, r.Code, code, r.Body.String())
	}
	var env struct{ Data json.RawMessage }
	e := json.Unmarshal(r.Body.Bytes(), &env)
	return env.Data, e
}
func TestConsoleLifecycle(t *testing.T) {
	if os.Getenv("XZDP_INTEGRATION") != "1" {
		t.Skip("requires real PostGIS and Redis")
	}
	t.Setenv("DEMO_CODES", "true")
	t.Setenv("MOCK_PAYMENT", "true")
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	e := fixture(ctx, func(s *store.Store) error {
		sale := trade.Sale{DB: s.DB, Redis: s.Redis, Prefix: s.Prefix + "trade:", Buckets: 4}
		if e := sale.InitGroup(ctx); e != nil {
			return e
		}
		h := server.New()
		httpapi.Register(h, s, sale)
		b := browser{h: h}
		get := func(path string) (json.RawMessage, error) { return b.request("GET", path, "", 200) }
		post := func(path, body string) (json.RawMessage, error) { return b.request("POST", path, body, 200) }
		if _, e := b.request("GET", "/orders", "", 401); e != nil {
			return e
		}
		if _, e := get("/shops/search?x=120.15&y=30.25"); e != nil {
			return e
		}
		if _, e := get("/shops/1"); e != nil {
			return e
		}
		raw, e := post("/user/email/code", `{"email":"demo@example.com","purpose":"login"}`)
		if e != nil {
			return e
		}
		var data struct{ DemoCode string }
		if e = json.Unmarshal(raw, &data); e != nil {
			return e
		}
		body := fmt.Sprintf(`{"email":"demo@example.com","code":"%s"}`, data.DemoCode)
		raw, e = post("/user/email/login", body)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &b.token); e != nil {
			return e
		}
		if _, e = b.request("POST", "/user/email/login", body, 400); e != nil {
			return e
		}
		if _, e = post("/user/sign", "{}"); e != nil {
			return e
		}
		if _, e = get("/user/sign/reward"); e != nil {
			return e
		}
		// An existing monthly rule may override full attendance. Grant exactly once on repeat sign-ins.
		_, e = s.DB.ExecContext(ctx, "INSERT INTO sign_reward_rule VALUES(date_trunc('month',now() at time zone 'Asia/Shanghai')::date,1,1)")
		if e != nil {
			return e
		}
		for range 2 {
			raw, e = post("/user/sign", "{}")
			if e != nil {
				return e
			}
			var progress struct {
				Days    int
				Claimed bool
			}
			if e = json.Unmarshal(raw, &progress); e != nil || progress.Days != 1 || !progress.Claimed {
				return fmt.Errorf("duplicate daily sign or missing reward: %s", raw)
			}
		}
		wallet, e := get("/wallet")
		if e != nil || !bytes.Contains(wallet, []byte("满勤")) {
			return fmt.Errorf("missing earned voucher %s: %v", wallet, e)
		}
		first, e := post("/slots/1/bookings", `{"partySize":2}`)
		if e != nil {
			return e
		}
		again, e := post("/slots/1/bookings", `{"partySize":2}`)
		if e != nil || !bytes.Equal(first, again) {
			return fmt.Errorf("booking retry: %s %s %v", first, again, e)
		}
		if _, e = post("/bookings/1/cancel", "{}"); e != nil {
			return e
		}
		note, e := post("/notes", `{"title":"山隙","content":"窗边座位","shopID":1}`)
		if e != nil {
			return e
		}
		if _, e = post("/notes/"+string(note)+"/like", "{}"); e != nil {
			return e
		}
		if _, e = post("/notes/"+string(note)+"/comments", `{"content":"周末见"}`); e != nil {
			return e
		}
		if _, e = post("/vouchers/1/buy", "{}"); e != nil {
			return e
		}
		raw, e = post("/vouchers/2/buy", "{}")
		if e != nil {
			return e
		}
		var id string
		if e = json.Unmarshal(raw, &id); e != nil {
			return e
		}
		if status, e := get("/orders/" + id); e != nil || string(status) != `"queued"` {
			return fmt.Errorf("queued %s: %v", status, e)
		}
		if _, e = sale.Read(ctx, "worker"); e != nil {
			return e
		}
		paid, e := post("/orders/"+id+"/pay", "{}")
		if e != nil {
			return e
		}
		paidAgain, e := post("/orders/"+id+"/pay", "{}")
		if e != nil || !bytes.Equal(paid, paidAgain) {
			return fmt.Errorf("payment replay %v", e)
		}
		if _, e = b.request("POST", "/orders/"+id+"/redeem", string(paid), 400); e != nil {
			return e
		}
		_, e = s.DB.ExecContext(ctx, "INSERT INTO shop_staff SELECT 2,user_id FROM user_email WHERE email='demo@example.com'")
		if e != nil {
			return e
		}
		if _, e = post("/orders/"+id+"/redeem", string(paid)); e != nil {
			return e
		}
		if _, e = b.request("POST", "/orders/"+id+"/redeem", string(paid), 400); e != nil {
			return e
		}
		// Redeliver the committed order and confirm the durable stock is unchanged.
		buyer, e := s.Redis.Get(ctx, s.Prefix+"auth:session:"+b.token).Int64()
		if e != nil {
			return e
		}
		stream := sale.Stream()
		if e = s.Redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"id": id, "user": buyer, "voucher": 2, "bucket": buyer % 4}}).Err(); e != nil {
			return e
		}
		if _, e = sale.Read(ctx, "worker"); e != nil {
			return e
		}
		second, e := sale.Reserve(ctx, 2, buyer+1)
		if e != nil {
			return e
		}
		if _, e = sale.Read(ctx, "worker"); e != nil {
			return e
		}
		_, e = s.DB.ExecContext(ctx, "UPDATE tb_voucher_order SET create_time=now()-interval '16 minutes' WHERE id=$1", second)
		if e != nil {
			return e
		}
		for range 2 {
			if e = sale.Expire(ctx); e != nil {
				return e
			}
		}
		var stock int
		if e = s.DB.QueryRowContext(ctx, "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=2").Scan(&stock); e != nil || stock != 99 {
			return fmt.Errorf("stock after release %d: %v", stock, e)
		}
		oid, e := strconv.ParseInt(second, 10, 64)
		if e != nil {
			return e
		}
		if state, e := sale.Status(ctx, oid, buyer+1); e != nil || state != "cancelled" {
			return fmt.Errorf("expired %s: %v", state, e)
		}
		if e = s.Redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"malformed": "test"}}).Err(); e != nil {
			return e
		}
		if _, e = sale.Read(ctx, "worker"); e != nil {
			return e
		}
		if n, e := s.Redis.XLen(ctx, stream+":dead").Result(); e != nil || n != 1 {
			return fmt.Errorf("dead letters %d: %v", n, e)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}

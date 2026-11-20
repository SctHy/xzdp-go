package social

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

func TestLikeFlushRetriesAndKeepsNewerVersion(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := Store{DB: db, Redis: r, Prefix: "test:"}
	m.ExpectQuery("SELECT COALESCE").WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"liked", "version", "user_id"}).AddRow(4, 0, 3))
	on, err := s.Toggle(ctx, 7, 11)
	if err != nil || !on {
		t.Fatalf("first toggle %t %v", on, err)
	}
	m.ExpectExec("UPDATE tb_blog").WillReturnError(errors.New("database unavailable"))
	if err = s.Flush(ctx); err == nil {
		t.Fatal("expected failed flush")
	}
	on, err = s.Toggle(ctx, 7, 11)
	if err != nil || on {
		t.Fatalf("second toggle %t %v", on, err)
	}
	key := s.countKey(7)
	if err = clean.Run(ctx, r, []string{key, s.Prefix + "dirty"}, 1).Err(); err != nil {
		t.Fatal(err)
	}
	if !r.SIsMember(ctx, s.Prefix+"dirty", key).Val() {
		t.Fatal("old snapshot cleared newer toggle")
	}
	m.ExpectExec("UPDATE tb_blog").WithArgs("4", "2", 7, "2").WillReturnResult(sqlmock.NewResult(0, 1))
	if err = s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Count(ctx, 7, -1) != 4 || r.SCard(ctx, s.Prefix+"dirty").Val() != 0 {
		t.Fatal("count did not settle")
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

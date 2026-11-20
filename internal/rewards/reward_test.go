package rewards

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestCumulativeNonConsecutiveDaysAndMonthBoundary(t *testing.T) {
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer r.Close()
	db, m, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := service{db, r}
	for _, day := range []int{1, 3, 5, 8, 10, 12, 15} {
		require.NoError(t, r.SetBit(ctx, "user:sign:5:202611", int64(day-1), 1).Err())
	}
	for _, date := range []time.Time{time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC)} {
		month := date.Format("2006-01") + "-01"
		m.ExpectBegin()
		m.ExpectQuery("SELECT COALESCE").WithArgs(month).WillReturnRows(sqlmock.NewRows([]string{"days", "id"}).AddRow(7, 1))
		if date.Month() == 11 {
			m.ExpectExec("INSERT INTO sign_reward_grant").WithArgs(5, month, 1).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT voucher_id").WillReturnRows(sqlmock.NewRows([]string{"voucher"}).AddRow(1))
			m.ExpectCommit()
			v, e := s.progress(ctx, 5, date, true)
			require.NoError(t, e)
			require.Equal(t, 7, v.Days)
		} else {
			m.ExpectQuery("SELECT voucher_id").WillReturnRows(sqlmock.NewRows([]string{"voucher"}))
			m.ExpectRollback()
			_, e := s.progress(ctx, 5, date, true)
			require.Error(t, e)
		}
	}
	require.NoError(t, m.ExpectationsWereMet())
}

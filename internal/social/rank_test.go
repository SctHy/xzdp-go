package social

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestHotRankSnapshotSurvivesFailedRefresh(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := Store{DB: db, Redis: r, Prefix: "test:"}
	m.ExpectQuery("SELECT b.id").WithArgs(100).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(21).AddRow(17))
	require.NoError(t, s.RefreshRank(ctx))
	for n := 0; n < 10; n++ {
		ids, err := s.RankPage(ctx, 1, 10)
		require.NoError(t, err)
		require.Equal(t, []int64{21, 17}, ids)
	}
	mr.FastForward(45 * time.Second)
	m.ExpectQuery("SELECT b.id").WillReturnError(context.DeadlineExceeded)
	require.Error(t, s.RefreshRank(ctx))
	ids, err := s.RankPage(ctx, 1, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{21, 17}, ids)
	_, err = s.RankPage(ctx, 11, 10)
	require.Error(t, err)
	require.NoError(t, m.ExpectationsWereMet())
}

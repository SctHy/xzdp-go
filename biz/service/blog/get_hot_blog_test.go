package blog

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
	"xzdp/conf"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	goredis "github.com/go-redis/redis/v8"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	pgDal "xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
	blog "xzdp/biz/model/blog"
)

func TestGetHotBlogService_Run(t *testing.T) {
	// Mock Redis
	mr, _ := miniredis.Run()
	redis.RedisClient = goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	// Mock MySQL
	db, mock, _ := sqlmock.New()
	gormDB, _ := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	pgDal.DB = gormDB

	mock.ExpectQuery("SELECT b.*JOIN tb_user").WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "author_name", "author_icon"}).AddRow(1, 2, "author", "photo"))
	ctx := context.Background()
	c := app.NewContext(1)
	s := NewGetHotBlogService(ctx, c)

	req := &blog.BlogReq{}
	resp, err := s.Run(req)

	if err != nil {
		t.Fatal(err)
	}
	if len(*resp) != 1 || (*resp)[0].NickName != "author" || (*resp)[0].Icon != "photo" {
		t.Fatalf("unexpected blogs: %+v", resp)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHotPagesHydrateSnapshotInRankOrder(t *testing.T) {
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()
	redis.RedisClient = client
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	pgDal.DB, err = gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, client.Set(ctx, "xzdp:"+conf.GetEnv()+":social:hot-rank", "[9,3]", time.Minute).Err())
	svc := NewGetHotBlogService(ctx, app.NewContext(1))
	for i := 0; i < 10; i++ {
		m.ExpectQuery("SELECT .* WHERE b.id IN").WithArgs(9, 3).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(3).AddRow(9))
		rows, err := svc.Run(&blog.BlogReq{Current: 1})
		require.NoError(t, err)
		require.Len(t, *rows, 2)
		require.EqualValues(t, 9, (*rows)[0].ID)
	}
	require.NoError(t, m.ExpectationsWereMet())
}

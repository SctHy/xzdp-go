package shop

import (
	"context"
	"regexp"
	"testing"
	"xzdp/conf"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	goredis "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	pgDal "xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
)

func TestShopInfoService_Run(t *testing.T) {
	// Mock Redis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	redis.RedisClient = rdb
	pool := redsyncgoredis.NewPool(rdb)
	redis.RedsyncClient = redsync.New(pool)

	// Mock MySQL
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pgDal.DB = gormDB
	ConfigureCache(conf.GetConf())

	var shopId int64 = 1
	ctx := context.Background()
	c := app.NewContext(1)
	s := NewShopInfoService(ctx, c)

	// Case 1: Cache Miss, DB Hit
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM \"tb_shop\" WHERE \"tb_shop\".\"id\" = $1 ORDER BY \"tb_shop\".\"id\" LIMIT $2")).
		WithArgs(shopId, 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "Test Shop"))

	resp, err := s.Run(shopId)

	assert.DeepEqual(t, nil, err)
	assert.NotEqual(t, nil, resp)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

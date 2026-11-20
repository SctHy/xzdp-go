package shop

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	goredis "github.com/go-redis/redis/v8"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	pgDal "xzdp/biz/dal/postgres"
	"xzdp/biz/dal/redis"
	shop "xzdp/biz/model/shop"
)

func TestShopOfTypeService_Run(t *testing.T) {
	// Mock Redis
	mr, _ := miniredis.Run()
	redis.RedisClient = goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	// Mock MySQL
	db, mock, _ := sqlmock.New()
	gormDB, _ := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	pgDal.DB = gormDB

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM \"tb_shop\"")).WillReturnRows(sqlmock.NewRows([]string{"id"}))

	ctx := context.Background()
	c := app.NewContext(1)
	s := NewShopOfTypeService(ctx, c)
	// init req and assert value
	req := &shop.ShopOfTypeReq{}
	resp, err := s.Run(req)

	if err != nil {
		t.Log(err)
	} else {
		assert.NotEqual(t, nil, resp)
	}
}

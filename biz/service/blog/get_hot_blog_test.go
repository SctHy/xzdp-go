package blog

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	goredis "github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	mysqlDal "xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
	blog "xzdp/biz/model/blog"
)

func TestGetHotBlogService_Run(t *testing.T) {
	// Mock Redis
	mr, _ := miniredis.Run()
	redis.RedisClient = goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	// Mock MySQL
	db, mock, _ := sqlmock.New()
	gormDB, _ := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	mysqlDal.DB = gormDB

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

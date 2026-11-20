package blog

import (
	"context"

	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app"
	goredis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"xzdp/biz/model/user"
	"xzdp/biz/utils"

	mysqlDal "xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
)

func TestDeleteBlogService_Run(t *testing.T) {
	// Mock Redis
	mr := miniredis.RunT(t)
	redis.RedisClient = goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	// Mock MySQL
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	gormDB, _ := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	mysqlDal.DB = gormDB

	ctx := utils.SaveUser(context.Background(), &user.UserDTO{ID: 2})
	service := NewDeleteBlogService(ctx, app.NewContext(1))
	id := "1"
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `tb_blog` WHERE id=.+ AND user_id=").WithArgs(1, 2).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	_, err = service.Run(&id)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `tb_blog` WHERE id=.+ AND user_id=").WithArgs(1, 2).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM tb_blog_comments").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM feed_publication").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	_, err = service.Run(&id)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

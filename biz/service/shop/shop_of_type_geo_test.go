package shop

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

	mysqlDal "xzdp/biz/dal/mysql"
	"xzdp/biz/dal/redis"
	"xzdp/biz/model/shop"
	"xzdp/conf"
)

func TestNearbyReadsOnlyMatchedIDs(t *testing.T) {
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()
	redis.RedisClient = client
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mysqlDal.DB, err = gorm.Open(mysql.New(mysql.Config{Conn: db, SkipInitializeWithVersion: true}), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, client.GeoAdd(ctx, "xzdp:"+conf.GetEnv()+":geo:1", &goredis.GeoLocation{Name: "7", Longitude: 120, Latitude: 30}).Err())
	svc := NewShopOfTypeGeoService(ctx, app.NewContext(1))
	for n := 0; n < 10; n++ {
		m.ExpectQuery("SELECT .* FROM `tb_shop` WHERE id IN").WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"id", "type_id"}).AddRow(7, 1))
		got, err := svc.Run(&shop.ShopOfTypeGeoReq{Current: 1, TypeId: 1, Longitude: 120, Latitude: 30, Distance: 100})
		require.NoError(t, err)
		require.Len(t, *got, 1)
	}
	require.NoError(t, m.ExpectationsWereMet())
}

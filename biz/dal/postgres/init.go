package postgres

import (
	"context"
	"database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
	"xzdp/conf"
)

var DB *gorm.DB

func Open(ctx context.Context) (*sql.DB, error) {
	pool, err := sql.Open("pgx", conf.GetConf().Postgres.DSN)
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(4)
	pool.SetConnMaxLifetime(5 * time.Minute)
	if err = pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	DB, err = gorm.Open(postgres.New(postgres.Config{Conn: pool}),
		&gorm.Config{
			DisableAutomaticPing:   true,
			PrepareStmt:            true,
			SkipDefaultTransaction: true,
			NamingStrategy: schema.NamingStrategy{
				SingularTable: true,
				TablePrefix:   "tb_",
			},
		},
	)
	if err != nil {
		pool.Close()
		return nil, err
	}
	//全局注册创建时间
	DB.Callback().Create().Before("gorm:create").Register("update_timestamps_before_create", func(tx *gorm.DB) {
		if tx.Error == nil {
			now := time.Now().Format("2006-01-02 15:04:05")
			if tx.Statement.Schema != nil {
				if createTimeField := tx.Statement.Schema.LookUpField("create_time"); createTimeField != nil {
					createTimeField.Set(tx.Statement.Context, tx.Statement.ReflectValue, now)
				}
				if createTimeField := tx.Statement.Schema.LookUpField("update_time"); createTimeField != nil {
					createTimeField.Set(tx.Statement.Context, tx.Statement.ReflectValue, now)
				}
			}
		}
	})
	//全局注册更新时间
	DB.Callback().Update().Before("gorm:update").Register("update_timestamps_before_update", func(tx *gorm.DB) {
		if tx.Error == nil {
			now := time.Now().Format("2006-01-02 15:04:05")
			if tx.Statement.Schema != nil {
				if updateTimeField := tx.Statement.Schema.LookUpField("update_time"); updateTimeField != nil {
					_ = updateTimeField.Set(tx.Statement.Context, tx.Statement.ReflectValue, now)
				}
			}
		}
	})
	return pool, nil
}

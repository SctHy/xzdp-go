package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

var (
	conf      *Config
	configErr error
	once      sync.Once
)

type Config struct {
	MockPayment   bool   `yaml:"mock_payment"`
	FeedThreshold int64  `yaml:"feed_threshold"`
	KafkaBroker   string `yaml:"kafka_broker"`
	Env           string
	ShopCache     ShopCache `yaml:"shop_cache"`

	Hertz Hertz `yaml:"hertz"`
	MySQL MySQL `yaml:"mysql"`
	Redis Redis `yaml:"redis"`
	Cors  Cors  `yaml:"cors"`
}

type ShopCache struct {
	MemoryMiB      int     `yaml:"memory_mib"`
	LocalSeconds   int     `yaml:"local_seconds"`
	RedisSeconds   int     `yaml:"redis_seconds"`
	RefreshSeconds int     `yaml:"refresh_seconds"`
	HotIDs         []int64 `yaml:"hot_ids"`
	TrustBloom     bool    `yaml:"trust_bloom"`
}

func DefaultShopCache() ShopCache {
	return ShopCache{MemoryMiB: 16, LocalSeconds: 30, RedisSeconds: 600, RefreshSeconds: 60, HotIDs: []int64{1}}
}

type MySQL struct {
	DSN string `yaml:"dsn"`
}

type Redis struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
}

type Hertz struct {
	Address         string `yaml:"address"`
	EnablePprof     bool   `yaml:"enable_pprof"`
	EnableGzip      bool   `yaml:"enable_gzip"`
	EnableAccessLog bool   `yaml:"enable_access_log"`
	LogLevel        string `yaml:"log_level"`
	LogFileName     string `yaml:"log_file_name"`
	LogMaxSize      int    `yaml:"log_max_size"`
	LogMaxBackups   int    `yaml:"log_max_backups"`
	LogMaxAge       int    `yaml:"log_max_age"`
}
type CORSWhitelist struct {
	AllowOrigin      string `yaml:"allow-origin"`
	AllowMethods     string `yaml:"allow-methods"`
	AllowHeaders     string `yaml:"allow-headers"`
	ExposeHeaders    string `yaml:"expose-headers"`
	AllowCredentials bool   `yaml:"allow-credentials"`
}

type Cors struct {
	Mode      string          `yaml:"mode"`
	WhiteList []CORSWhitelist `yaml:"whitelist"`
}

// GetConf gets configuration instance
func GetConf() *Config {
	c, err := Current()
	if err != nil {
		panic(err)
	}
	return c
}

func Current() (*Config, error) {
	once.Do(func() { conf, configErr = Load() })
	return conf, configErr
}

func Load() (*Config, error) {
	c := Config{FeedThreshold: 10000, Env: GetEnv(), KafkaBroker: "127.0.0.1:9092", ShopCache: DefaultShopCache(), Hertz: Hertz{Address: ":8081", LogLevel: "info", EnableAccessLog: true}, MySQL: MySQL{DSN: "xzdp:xzdp-local@tcp(127.0.0.1:3306)/xzdp?parseTime=true&timeout=3s"}, Redis: Redis{Address: "127.0.0.1:6379"}, Cors: Cors{Mode: "allow-all"}}
	path, explicit := os.LookupEnv("XZDP_CONFIG")
	if !explicit {
		path = filepath.Join("conf", c.Env, "conf.yaml")
	}
	data, err := os.ReadFile(path)
	if err == nil {
		if err = yaml.UnmarshalStrict(data, &c); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	} else if explicit || !os.IsNotExist(err) {
		return nil, err
	}
	for key, dest := range map[string]*string{"XZDP_KAFKA_BROKER": &c.KafkaBroker, "XZDP_HTTP_ADDRESS": &c.Hertz.Address, "XZDP_LOG_LEVEL": &c.Hertz.LogLevel, "XZDP_MYSQL_DSN": &c.MySQL.DSN, "XZDP_REDIS_ADDRESS": &c.Redis.Address, "XZDP_REDIS_PASSWORD": &c.Redis.Password} {
		if v, ok := os.LookupEnv(key); ok {
			*dest = v
		}
	}
	if v, ok := os.LookupEnv("XZDP_ACCESS_LOG"); ok {
		c.Hertz.EnableAccessLog, err = strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
	}
	if v, ok := os.LookupEnv("XZDP_MOCK_PAYMENT"); ok {
		c.MockPayment, err = strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
	}
	if v, ok := os.LookupEnv("XZDP_PPROF"); ok {
		c.Hertz.EnablePprof, err = strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
	}
	if c.Hertz.Address == "" || c.Redis.Address == "" || c.MySQL.DSN == "" {
		return nil, fmt.Errorf("empty HTTP, MySQL or Redis configuration")
	}
	if c.ShopCache.MemoryMiB < 1 || c.ShopCache.MemoryMiB > 64 || c.ShopCache.LocalSeconds < 1 || c.ShopCache.RedisSeconds < 1 || c.ShopCache.RefreshSeconds < 1 {
		return nil, fmt.Errorf("invalid shop_cache capacity or TTL")
	}
	return &c, validator.Validate(&c)
}

func GetEnv() string {
	e := os.Getenv("GO_ENV")
	if len(e) == 0 {
		return "test"
	}
	return e
}

func LogLevel() hlog.Level {
	level := GetConf().Hertz.LogLevel
	switch level {
	case "trace":
		return hlog.LevelTrace
	case "debug":
		return hlog.LevelDebug
	case "info":
		return hlog.LevelInfo
	case "notice":
		return hlog.LevelNotice
	case "warn":
		return hlog.LevelWarn
	case "error":
		return hlog.LevelError
	case "fatal":
		return hlog.LevelFatal
	default:
		return hlog.LevelInfo
	}
}

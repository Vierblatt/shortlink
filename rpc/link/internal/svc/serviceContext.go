package svc

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"golink/common/bloom"
	"golink/common/model"
	"golink/common/mq"
	"golink/common/snowflake"
	"golink/rpc/link/internal/config"
)

type ServiceContext struct {
	Config        config.Config
	DB            *gorm.DB
	RedisClient   *redis.Client
	BloomFilter   *bloom.BloomFilter
	Snowflake     *snowflake.Snowflake
	KafkaProducer *mq.Producer
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gorm.Open(mysql.Open(c.MySQL.DataSource), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		panic(fmt.Sprintf("connect mysql: %v", err))
	}

	sqlDB, _ := db.DB()
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	db.AutoMigrate(&model.Link{}, &model.AccessLog{}, &model.LinkStat{})

	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Addr,
		Password: c.Redis.Password,
		DB:       c.Redis.DB,
	})

	bf := bloom.New(rdb, c.BloomFilter.Key, c.BloomFilter.Size, c.BloomFilter.HashFuncs)

	sf, err := snowflake.New(c.SnowflakeWorkerID)
	if err != nil {
		panic(fmt.Sprintf("init snowflake: %v", err))
	}

	kp := mq.NewProducer(c.Kafka.Brokers, c.Kafka.Topic)

	return &ServiceContext{
		Config:        c,
		DB:            db,
		RedisClient:   rdb,
		BloomFilter:   bf,
		Snowflake:     sf,
		KafkaProducer: kp,
	}
}

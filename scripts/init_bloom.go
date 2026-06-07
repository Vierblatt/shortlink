package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"golink/common/bloom"
	"golink/common/model"
)

func main() {
	dsn := flag.String("dsn", "root:root123@tcp(127.0.0.1:3306)/golink?charset=utf8mb4&parseTime=True", "MySQL DSN")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	bloomKey := flag.String("key", "golink:bloom", "Bloom filter Redis key")
	bloomSize := flag.Int64("size", 10000000, "Bloom filter bit size")
	hashFuncs := flag.Int("hash", 7, "Bloom filter hash count")
	flag.Parse()

	db, err := gorm.Open(mysql.Open(*dsn), &gorm.Config{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect mysql: %v\n", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	bf := bloom.New(rdb, *bloomKey, *bloomSize, *hashFuncs)

	var codes []string
	if err := db.Model(&model.Link{}).Pluck("short_code", &codes).Error; err != nil {
		fmt.Fprintf(os.Stderr, "query links: %v\n", err)
		os.Exit(1)
	}

	for _, code := range codes {
		if err := bf.Add([]byte(code)); err != nil {
			fmt.Fprintf(os.Stderr, "add bloom %s: %v\n", code, err)
			continue
		}
	}

	fmt.Printf("Loaded %d short codes into bloom filter\n", len(codes))
}

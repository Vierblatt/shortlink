package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"golink/common/model"
)

type AccessLogMessage struct {
	ShortCode string `json:"short_code"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	Referer   string `json:"referer"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	dsn := flag.String("dsn", "root:root123@tcp(127.0.0.1:3306)/golink?charset=utf8mb4&parseTime=True", "MySQL DSN")
	kafkaBroker := flag.String("kafka", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "access_logs", "Kafka topic")
	groupID := flag.String("group", "logconsumer", "Consumer group ID")
	flag.Parse()

	db, err := gorm.Open(mysql.Open(*dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		panic(fmt.Sprintf("connect mysql: %v", err))
	}

	sqlDB, _ := db.DB()
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetConnMaxLifetime(time.Hour)

	db.AutoMigrate(&model.Link{}, &model.AccessLog{}, &model.LinkStat{})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*kafkaBroker},
		GroupID:  *groupID,
		Topic:    *topic,
		MinBytes: 10,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	fmt.Printf("Log consumer started, listening on topic %s...\n", *topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Fprintf(os.Stderr, "read message: %v\n", err)
			continue
		}

		var logMsg AccessLogMessage
		if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
			fmt.Fprintf(os.Stderr, "unmarshal message: %v\n", err)
			continue
		}

		// write access log
		accessLog := model.AccessLog{
			ShortCode: logMsg.ShortCode,
			IP:        logMsg.IP,
			UserAgent: logMsg.UserAgent,
			Referer:   logMsg.Referer,
		}
		if err := db.Create(&accessLog).Error; err != nil {
			fmt.Fprintf(os.Stderr, "insert access log: %v\n", err)
			continue
		}

		// upsert link stats (daily PV/UV)
		today := time.Now().Format("2006-01-02")
		stat := model.LinkStat{
			ShortCode: logMsg.ShortCode,
			Date:      today,
			PV:        1,
			UV:        1,
		}
		db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "short_code"}, {Name: "date"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"pv":         gorm.Expr("link_stats.pv + 1"),
				"updated_at": time.Now(),
			}),
		}).Create(&stat)
	}
}

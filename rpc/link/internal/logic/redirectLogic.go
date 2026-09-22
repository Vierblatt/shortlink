package logic

import (
	"context"
	"fmt"
	"time"

	"golink/common/model"
	"golink/common/mq"
	"golink/rpc/link/internal/svc"
	"golink/rpc/link/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type RedirectLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRedirectLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RedirectLogic {
	return &RedirectLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RedirectLogic) Redirect(in *pb.RedirectRequest) (*pb.RedirectResponse, error) {
	code := in.ShortCode
	if code == "" {
		return nil, fmt.Errorf("short code is required")
	}

	// 1. Bloom filter - fast reject
	exists, _ := l.svcCtx.BloomFilter.Test([]byte(code))
	if !exists {
		return nil, fmt.Errorf("short link not found")
	}

	// 访问日志在确认命中之后立即异步投递，两条返回路径（缓存命中 / 回源）
	// 都要投递，否则缓存命中的绝大多数请求不会被统计到。
	// 放在这里而非各 return 之前，是为了让"命中"这个判定只写一次。
	sendLog := func() {
		msg := &mq.AccessLogMessage{
			ShortCode: code,
			IP:        in.Ip,
			UserAgent: in.UserAgent,
			Referer:   in.Referer,
			Timestamp: time.Now().Unix(),
		}
		// 用 context.Background 而非请求 ctx：请求返回后 ctx 会被取消，
		// 会导致异步投递被中断。
		go func() {
			if err := l.svcCtx.KafkaProducer.SendAccessLog(context.Background(), msg); err != nil {
				logx.Errorf("send access log: %v", err)
			}
		}()
	}

	// 2. Redis cache
	longURL, err := l.svcCtx.RedisClient.Get(l.ctx, cacheKey(code)).Result()
	if err == nil && longURL != "" {
		sendLog()
		return &pb.RedirectResponse{LongUrl: longURL}, nil
	}

	// 3. MySQL
	var link model.Link
	err = l.svcCtx.DB.WithContext(l.ctx).Where("short_code = ? AND status = 1", code).First(&link).Error
	if err != nil {
		return nil, fmt.Errorf("short link not found")
	}

	if link.ExpireAt != nil && time.Now().After(*link.ExpireAt) {
		return nil, fmt.Errorf("short link has expired")
	}

	// write back cache
	ttl := time.Duration(l.svcCtx.Config.CacheTTL) * time.Second
	l.svcCtx.RedisClient.Set(l.ctx, cacheKey(code), link.LongURL, ttl)

	sendLog()

	return &pb.RedirectResponse{LongUrl: link.LongURL}, nil
}

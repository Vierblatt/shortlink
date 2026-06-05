package logic

import (
	"context"
	"fmt"
	"time"

	"golink/common/model"
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

	// 2. Redis cache
	longURL, err := l.svcCtx.RedisClient.Get(l.ctx, cacheKey(code)).Result()
	if err == nil && longURL != "" {
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

	return &pb.RedirectResponse{LongUrl: link.LongURL}, nil
}

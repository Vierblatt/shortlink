package logic

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"golink/common/base62"
	"golink/common/model"
	"golink/rpc/link/internal/svc"
	"golink/rpc/link/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ShortenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShortenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShortenLogic {
	return &ShortenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShortenLogic) Shorten(in *pb.ShortenRequest) (*pb.ShortenResponse, error) {
	longURL := strings.TrimSpace(in.LongUrl)
	if longURL == "" {
		return nil, fmt.Errorf("url is required")
	}

	if !strings.HasPrefix(longURL, "http://") && !strings.HasPrefix(longURL, "https://") {
		longURL = "https://" + longURL
	}
	if _, err := url.ParseRequestURI(longURL); err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}

	link := &model.Link{
		LongURL: longURL,
		UserID:  0,
		Status:  1,
	}

	if in.CustomCode != "" {
		var existing model.Link
		err := l.svcCtx.DB.WithContext(l.ctx).Where("short_code = ?", in.CustomCode).First(&existing).Error
		if err == nil {
			return nil, fmt.Errorf("short code already exists")
		}

		link.ID = uint64(l.svcCtx.Snowflake.Generate())
		link.ShortCode = in.CustomCode
	} else {
		id := uint64(l.svcCtx.Snowflake.Generate())
		link.ID = id
		link.ShortCode = base62.Encode(id)
	}

	if in.ExpireAt > 0 {
		t := time.Unix(in.ExpireAt, 0)
		link.ExpireAt = &t
	}
	if in.Password != "" {
		link.Password = in.Password
	}

	if err := l.svcCtx.DB.WithContext(l.ctx).Create(link).Error; err != nil {
		return nil, fmt.Errorf("create link: %w", err)
	}

	// write to bloom filter
	l.svcCtx.BloomFilter.Add([]byte(link.ShortCode))

	// cache in redis
	ttl := time.Duration(l.svcCtx.Config.CacheTTL) * time.Second
	l.svcCtx.RedisClient.Set(l.ctx, cacheKey(link.ShortCode), link.LongURL, ttl)

	shortURL := strings.TrimRight(l.svcCtx.Config.ShortLinkDomain, "/") + "/" + link.ShortCode

	return &pb.ShortenResponse{
		ShortCode: link.ShortCode,
		ShortUrl:  shortURL,
	}, nil
}

func cacheKey(code string) string {
	return "link:" + code
}

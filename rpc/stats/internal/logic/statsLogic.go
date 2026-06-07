package logic

import (
	"context"
	"fmt"

	"golink/common/model"
	"golink/rpc/stats/internal/svc"
	"golink/rpc/stats/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type StatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatsLogic {
	return &StatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StatsLogic) GetStats(in *pb.StatsRequest) (*pb.StatsResponse, error) {
	code := in.ShortCode
	if code == "" {
		return nil, fmt.Errorf("short code is required")
	}

	var link model.Link
	if err := l.svcCtx.DB.WithContext(l.ctx).Where("short_code = ?", code).First(&link).Error; err != nil {
		return nil, fmt.Errorf("short link not found")
	}

	var stat model.LinkStat
	l.svcCtx.DB.WithContext(l.ctx).Where("short_code = ?", code).First(&stat)

	return &pb.StatsResponse{
		ShortCode: link.ShortCode,
		LongUrl:   link.LongURL,
		Pv:        int64(stat.PV),
		Uv:        int64(stat.UV),
	}, nil
}

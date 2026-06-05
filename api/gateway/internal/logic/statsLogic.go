package logic

import (
	"context"

	"golink/api/gateway/internal/svc"
	"golink/api/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type StatsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatsLogic {
	return &StatsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *StatsLogic) Stats() (resp *types.StatsResp, err error) {
	// TODO: implement in Phase 2 when Stats RPC is ready
	return &types.StatsResp{}, nil
}

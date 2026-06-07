package logic

import (
	"context"

	"golink/api/gateway/internal/svc"
	"golink/api/gateway/internal/types"
	statspb "golink/rpc/stats/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type StatsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
	code   string
}

func NewStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatsLogic {
	return &StatsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *StatsLogic) SetCode(code string) *StatsLogic {
	l.code = code
	return l
}

func (l *StatsLogic) Stats() (resp *types.StatsResp, err error) {
	rpcResp, err := l.svcCtx.StatsRpc.GetStats(l.ctx, &statspb.StatsRequest{
		ShortCode: l.code,
	})
	if err != nil {
		return nil, err
	}

	return &types.StatsResp{
		Code:       rpcResp.ShortCode,
		LongURL:    rpcResp.LongUrl,
		ClickCount: uint64(rpcResp.Pv),
		PV:         int32(rpcResp.Pv),
		UV:         int32(rpcResp.Uv),
	}, nil
}

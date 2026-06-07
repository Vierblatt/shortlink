package server

import (
	"context"

	"golink/rpc/stats/internal/logic"
	"golink/rpc/stats/internal/svc"
	"golink/rpc/stats/pb"
)

type StatsServer struct {
	svcCtx *svc.ServiceContext
	pb.UnimplementedStatsServer
}

func NewStatsServer(svcCtx *svc.ServiceContext) *StatsServer {
	return &StatsServer{
		svcCtx: svcCtx,
	}
}

func (s *StatsServer) GetStats(ctx context.Context, in *pb.StatsRequest) (*pb.StatsResponse, error) {
	l := logic.NewStatsLogic(ctx, s.svcCtx)
	return l.GetStats(in)
}

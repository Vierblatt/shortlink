package svc

import (
	"golink/api/gateway/internal/config"
	"golink/rpc/link/pb"
	statspb "golink/rpc/stats/pb"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config   config.Config
	LinkRpc  pb.LinkClient
	StatsRpc statspb.StatsClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	linkClient := zrpc.MustNewClient(c.LinkRpc)
	statsClient := zrpc.MustNewClient(c.StatsRpc)
	return &ServiceContext{
		Config:   c,
		LinkRpc:  pb.NewLinkClient(linkClient.Conn()),
		StatsRpc: statspb.NewStatsClient(statsClient.Conn()),
	}
}

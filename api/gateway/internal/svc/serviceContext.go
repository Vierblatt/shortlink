package svc

import (
	"golink/api/gateway/internal/config"
	"golink/rpc/link/pb"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config  config.Config
	LinkRpc pb.LinkClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	client := zrpc.MustNewClient(c.LinkRpc)
	return &ServiceContext{
		Config:  c,
		LinkRpc: pb.NewLinkClient(client.Conn()),
	}
}

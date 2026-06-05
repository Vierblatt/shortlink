package logic

import (
	"context"

	"golink/api/gateway/internal/svc"
	"golink/api/gateway/internal/types"
	"golink/rpc/link/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ShortenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewShortenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShortenLogic {
	return &ShortenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ShortenLogic) Shorten(req *types.ShortenReq) (resp *types.ShortenResp, err error) {
	rpcResp, err := l.svcCtx.LinkRpc.Shorten(l.ctx, &pb.ShortenRequest{
		LongUrl:    req.URL,
		CustomCode: req.CustomCode,
		ExpireAt:   req.ExpireAt,
		Password:   req.Password,
	})
	if err != nil {
		return nil, err
	}

	return &types.ShortenResp{
		ShortURL: rpcResp.ShortUrl,
		Code:     rpcResp.ShortCode,
	}, nil
}

package logic

import (
	"context"

	"golink/api/gateway/internal/svc"
	"golink/api/gateway/internal/types"
	"golink/rpc/link/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type RedirectLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
	code   string
}

func NewRedirectLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RedirectLogic {
	return &RedirectLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RedirectLogic) SetCode(code string) *RedirectLogic {
	l.code = code
	return l
}

func (l *RedirectLogic) Redirect() (resp *types.RedirectResp, err error) {
	rpcResp, err := l.svcCtx.LinkRpc.Redirect(l.ctx, &pb.RedirectRequest{
		ShortCode: l.code,
	})
	if err != nil {
		return nil, err
	}

	return &types.RedirectResp{
		LongURL: rpcResp.LongUrl,
	}, nil
}

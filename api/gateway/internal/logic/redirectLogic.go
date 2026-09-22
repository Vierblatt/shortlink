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
	ip     string
	ua     string
	ref    string
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

// SetClientInfo 记录访问来源，随 Redirect 请求传给 link-rpc 用于写访问日志。
func (l *RedirectLogic) SetClientInfo(ip, userAgent, referer string) *RedirectLogic {
	l.ip = ip
	l.ua = userAgent
	l.ref = referer
	return l
}

func (l *RedirectLogic) Redirect() (resp *types.RedirectResp, err error) {
	rpcResp, err := l.svcCtx.LinkRpc.Redirect(l.ctx, &pb.RedirectRequest{
		ShortCode: l.code,
		Ip:        l.ip,
		UserAgent: l.ua,
		Referer:   l.ref,
	})
	if err != nil {
		return nil, err
	}

	return &types.RedirectResp{
		LongURL: rpcResp.LongUrl,
	}, nil
}

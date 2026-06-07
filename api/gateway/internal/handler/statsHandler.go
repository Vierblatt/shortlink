// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package handler

import (
	"fmt"
	"net/http"

	"golink/api/gateway/internal/logic"
	"golink/api/gateway/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/pathvar"
)

func StatsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code := pathvar.Vars(r)["code"]
		if code == "" {
			httpx.ErrorCtx(r.Context(), w, fmt.Errorf("short code is required"))
			return
		}

		l := logic.NewStatsLogic(r.Context(), svcCtx).SetCode(code)
		resp, err := l.Stats()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}

package handler

import (
	"net/http"

	"golink/api/gateway/internal/logic"
	"golink/api/gateway/internal/svc"

	"github.com/zeromicro/go-zero/rest/pathvar"
)

func RedirectHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code := pathvar.Vars(r)["code"]
		if code == "" {
			http.NotFound(w, r)
			return
		}

		l := logic.NewRedirectLogic(r.Context(), svcCtx).SetCode(code)
		resp, err := l.Redirect()
		if err != nil {
			http.NotFound(w, r)
			return
		}

		http.Redirect(w, r, resp.LongURL, http.StatusFound)
	}
}

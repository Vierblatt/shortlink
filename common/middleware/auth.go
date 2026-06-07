package middleware

import (
	"context"
	"net/http"
	"strings"

	"golink/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

type contextKey string

const (
	CtxKeyUserID   contextKey = "user_id"
	CtxKeyUsername contextKey = "username"
)

func AuthHandler(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				httpx.ErrorCtx(r.Context(), w, &authError{"missing authorization header"})
				return
			}

			token := strings.TrimPrefix(auth, "Bearer ")
			if token == auth {
				httpx.ErrorCtx(r.Context(), w, &authError{"invalid authorization format"})
				return
			}

			claims, err := utils.ParseToken(secret, token)
			if err != nil {
				logx.WithContext(r.Context()).Errorf("jwt parse: %v", err)
				httpx.ErrorCtx(r.Context(), w, &authError{"invalid or expired token"})
				return
			}

			ctx := context.WithValue(r.Context(), CtxKeyUserID, claims.UserID)
			ctx = context.WithValue(ctx, CtxKeyUsername, claims.Username)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

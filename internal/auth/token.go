// Package auth provides the deliberately small token-to-role boundary used by
// the single-instance control plane. It does not issue, refresh, or persist tokens.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type Role string

const (
	ViewerRole   Role = "viewer"
	OperatorRole Role = "operator"
)

var (
	ErrUnauthorized = errors.New("authentication required")
	ErrForbidden    = errors.New("insufficient role")
)

type contextKey struct{}

// ParseBearerToken accepts exactly "Bearer <token>" with one ASCII space.
func ParseBearerToken(header string) (string, error) {
	if header == "" || strings.Count(header, " ") != 1 {
		return "", ErrUnauthorized
	}
	parts := strings.SplitN(header, " ", 2)
	if parts[0] != "Bearer" || parts[1] == "" || strings.IndexFunc(parts[1], func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\r' || r == '\n'
	}) >= 0 {
		return "", ErrUnauthorized
	}
	return parts[1], nil
}

// Authenticator maps two configured tokens to the two roles supported in module 2.
type Authenticator struct {
	viewerToken   string
	operatorToken string
}

func NewAuthenticator(viewerToken, operatorToken string) (*Authenticator, error) {
	if strings.TrimSpace(viewerToken) == "" || strings.TrimSpace(operatorToken) == "" {
		return nil, fmt.Errorf("viewer and operator tokens are required")
	}
	if viewerToken == operatorToken {
		return nil, fmt.Errorf("viewer and operator tokens must differ")
	}
	return &Authenticator{viewerToken: viewerToken, operatorToken: operatorToken}, nil
}

func (a *Authenticator) Role(header string) (Role, error) {
	token, err := ParseBearerToken(header)
	if err != nil {
		return "", ErrUnauthorized
	}
	switch token {
	case a.viewerToken:
		return ViewerRole, nil
	case a.operatorToken:
		return OperatorRole, nil
	default:
		return "", ErrUnauthorized
	}
}

func WithRole(ctx context.Context, role Role) context.Context {
	return context.WithValue(ctx, contextKey{}, role)
}

func RoleFromContext(ctx context.Context) (Role, bool) {
	role, ok := ctx.Value(contextKey{}).(Role)
	return role, ok
}

// RequireRole checks the role already attached by an authentication middleware.
func RequireRole(role Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actual, ok := RoleFromContext(r.Context())
		if !ok || (role == OperatorRole && actual != OperatorRole) || (role == ViewerRole && actual != ViewerRole && actual != OperatorRole) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Authenticator) Middleware(required Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role, err := a.Role(r.Header.Get("Authorization"))
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if (required == OperatorRole && role != OperatorRole) || (required == ViewerRole && role != ViewerRole && role != OperatorRole) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithRole(r.Context(), role)))
	})
}

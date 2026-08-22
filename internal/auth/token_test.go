package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseBearerTokenRequiresOneBearerCredential(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "valid", header: "Bearer viewer-secret", want: "viewer-secret"},
		{name: "missing", header: "", want: ""},
		{name: "wrong scheme", header: "Basic viewer-secret", want: ""},
		{name: "multiple spaces", header: "Bearer  viewer-secret", want: ""},
		{name: "extra credential", header: "Bearer viewer-secret extra", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseBearerToken(test.header)
			if test.want == "" {
				if err == nil {
					t.Fatalf("ParseBearerToken() error = nil, token = %q", got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("ParseBearerToken() = %q, %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestAuthenticatorMapsTokensToRolesAndMiddlewareRejectsInsufficientRole(t *testing.T) {
	authenticator, err := NewAuthenticator("viewer-secret", "operator-secret")
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}
	if role, err := authenticator.Role("Bearer viewer-secret"); err != nil || role != ViewerRole {
		t.Fatalf("Role(viewer) = %q, %v", role, err)
	}
	if role, err := authenticator.Role("Bearer operator-secret"); err != nil || role != OperatorRole {
		t.Fatalf("Role(operator) = %q, %v", role, err)
	}

	handler := authenticator.Middleware(OperatorRole, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer viewer-secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("viewer on operator route status = %d, want 403", recorder.Code)
	}
}

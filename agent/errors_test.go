package agent

import (
	"errors"
	"testing"
)

func TestErrorsExposeStableCodes(t *testing.T) {
	err := &Error{Code: CodeToolNotAllowed, Message: "tool is not registered"}
	if got := CodeOf(err); got != CodeToolNotAllowed {
		t.Fatalf("CodeOf() = %q, want %q", got, CodeToolNotAllowed)
	}
	if got := CodeOf(errors.New("plain")); got != CodeInternal {
		t.Fatalf("CodeOf(plain) = %q, want %q", got, CodeInternal)
	}
}

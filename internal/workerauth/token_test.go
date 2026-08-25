package workerauth

import (
	"encoding/base64"
	"testing"
)

func TestTokenGenerationProvides256BitsAndStableDigest(t *testing.T) {
	first, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("two generated tokens are equal")
	}
	raw, err := base64.RawURLEncoding.DecodeString(first)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if len(raw) != 32 {
		t.Fatalf("decoded token bytes = %d, want 32", len(raw))
	}

	digest := DigestToken(first)
	if len(digest) != 32 {
		t.Fatalf("digest bytes = %d, want 32", len(digest))
	}
	if !MatchesToken(first, digest) {
		t.Fatal("MatchesToken() rejected matching token")
	}
	if MatchesToken(second, digest) || MatchesToken("", digest) || MatchesToken(first, digest[:1]) {
		t.Fatal("MatchesToken() accepted non-matching token or malformed digest")
	}
}

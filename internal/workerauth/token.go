// Package workerauth 提供 Worker 注册和租约处理共享的凭据基础操作。
package workerauth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
)

const tokenBytes = 32

// GenerateToken 返回包含 256 位密码学随机性的 URL 安全凭据。
func GenerateToken() (string, error) {
	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DigestToken 返回唯一允许持久化的 Token 摘要表示。
func DigestToken(token string) []byte {
	digest := sha256.Sum256([]byte(token))
	return append([]byte(nil), digest[:]...)
}

// MatchesToken 使用常量时间比较明文凭据和已保存的 SHA-256 摘要。
func MatchesToken(token string, expectedDigest []byte) bool {
	if token == "" || len(expectedDigest) != sha256.Size {
		return false
	}
	actual := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(actual[:], expectedDigest) == 1
}

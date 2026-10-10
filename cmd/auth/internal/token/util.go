package token

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
)

// NewToken returns the raw secret (send to client once) and its SHA-256 hash (store in DB).
// The caller is responsible for composing the full refresh token as sid + "." + raw.
func NewToken() (raw string, hash []byte, err error) {
	b := make([]byte, 32) // 256 bits
	if _, err = rand.Read(b); err != nil {
		return "", nil, err
	}
	raw = base64.RawURLEncoding.EncodeToString(b)

	sum := sha256.Sum256([]byte(raw))
	return raw, sum[:], nil
}

// Matches compares a presented secret against the stored hash in constant time.
func MatchesToken(secret string, stored []byte) bool {
	sum := sha256.Sum256([]byte(secret))
	return subtle.ConstantTimeCompare(sum[:], stored) == 1
}

func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

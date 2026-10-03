package token

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
)

// New returns the raw token (send to client once) and its hash (store in DB).
func NewToken(sid string) (raw string, hash []byte, err error) {
	b := make([]byte, 32) // 256 bits
	if _, err = rand.Read(b); err != nil {
		return "", nil, err
	}
	secret := base64.RawURLEncoding.EncodeToString(b)
	raw = sid + "." + secret // sid prefix for lookup

	sum := sha256.Sum256([]byte(secret))
	return raw, sum[:], nil
}

// Matches compares a presented secret against the stored hash in constant time.
func MatchesToken(secret string, stored []byte) bool {
	sum := sha256.Sum256([]byte(secret))
	return subtle.ConstantTimeCompare(sum[:], stored) == 1
}

package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

type jwksResponse struct {
	Keys []jwkKey `json:"keys"`
}

type jwkKey struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

type JWKS struct {
	PublicKey ed25519.PublicKey
	Kid       string
}

func parseEd25519PublicKey(x string) (ed25519.PublicKey, error) {
	trimmed := strings.TrimSpace(x)

	// Case 1: OpenSSH public key format (e.g. ssh-ed25519 AAAAC3...)
	if strings.HasPrefix(trimmed, "ssh-ed25519") {
		sshPub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(trimmed))
		if err == nil {
			if cryptoPub, ok := sshPub.(ssh.CryptoPublicKey); ok {
				if edPub, ok := cryptoPub.CryptoPublicKey().(ed25519.PublicKey); ok {
					return edPub, nil
				}
			}
		}
	}

	// Case 2: Base64 encodings (RFC 8037 raw URL encoding, standard URL, standard base64)
	encodings := []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	}

	for _, enc := range encodings {
		if b, err := enc.DecodeString(trimmed); err == nil && len(b) == ed25519.PublicKeySize {
			return ed25519.PublicKey(b), nil
		}
	}

	// Case 3: Raw 32 bytes
	if len([]byte(trimmed)) == ed25519.PublicKeySize {
		return ed25519.PublicKey([]byte(trimmed)), nil
	}

	return nil, fmt.Errorf("unable to parse Ed25519 public key from %q", x)
}

func FetchJWKS(jwksURL string) (*JWKS, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(jwksURL)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS from %s: %w", jwksURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS endpoint %s returned %d", jwksURL, resp.StatusCode)
	}

	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}

	for _, key := range jwks.Keys {
		if (key.Kty == "OKP" || key.Crv == "Ed25519") && (key.Use == "" || key.Use == "sig") {
			pubKey, err := parseEd25519PublicKey(key.X)
			if err != nil {
				continue
			}
			return &JWKS{
				PublicKey: pubKey,
				Kid:       key.Kid,
			}, nil
		}
	}
	return nil, fmt.Errorf("no usable Ed25519 signing key found in JWKS from %s", jwksURL)
}

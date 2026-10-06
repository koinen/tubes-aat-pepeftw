package auth

import (
	"crypto/ed25519"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"

	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

type Verifier struct {
	publicKey ed25519.PublicKey
	kid       string
	issuer    string
	audience  string
	denylist  *Denylist
}

func NewVerifier(jwks *JWKS, issuer, audience string, denylist *Denylist) *Verifier {
	return &Verifier{
		publicKey: jwks.PublicKey,
		kid:       jwks.Kid,
		issuer:    issuer,
		audience:  audience,
		denylist:  denylist,
	}
}

func (v *Verifier) Middleware(c fiber.Ctx) error {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "missing Authorization header")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "invalid Authorization header format")
	}
	tokenString := strings.TrimSpace(parts[1])

	token, err := jwt.Parse(tokenString, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != "EdDSA" {
			return nil, jwt.ErrSignatureInvalid
		}
		if v.kid != "" {
			if kid, ok := t.Header["kid"].(string); ok && kid != v.kid {
				return nil, jwt.ErrSignatureInvalid
			}
		}
		return v.publicKey, nil
	},
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithValidMethods([]string{"EdDSA"}),
	)

	if err != nil || !token.Valid {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "invalid or expired token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "invalid token claims")
	}

	// Check jti and denylist
	jti, _ := claims["jti"].(string)
	if jti == "" {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "token missing jti claim")
	}
	if v.denylist != nil && v.denylist.IsDenied(jti) {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "token has been revoked")
	}

	// Validate scope and sub claims
	scope, _ := claims["scope"].(string)
	sub, _ := claims["sub"].(string)
	if scope == "" || sub == "" {
		return common.WriteError(c, fiber.StatusUnauthorized, common.CodeUnauthorized, "token missing required claims")
	}

	common.SetScope(c, scope)
	common.SetSub(c, sub)

	return c.Next()
}

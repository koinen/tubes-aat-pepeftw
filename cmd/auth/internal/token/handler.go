package token

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
	"tubes-aat-pepeftw/cmd/auth/internal/common"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	Scope  string
	Secret string // filled from env at startup
}

type tokenHandler struct {
	clients     map[string]Client
	cfg         config.TokenConfig
	redisClient *redis.Client
	kafkaClient *kgo.Client
	keyId       string
	signingKey  ed25519.PrivateKey
	publicKeyX  string
}

func NewTokenHandler(
	cfg config.TokenConfig,
	redisClient *redis.Client,
	kafkaClient *kgo.Client,
) (*tokenHandler, error) {
	parsed, err := jwt.ParseEdPrivateKeyFromPEM([]byte(cfg.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	signingKey, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not Ed25519")
	}
	publicKey := signingKey.Public().(ed25519.PublicKey)

	clients := map[string]Client{
		"media":      {Scope: "hazard:read:summary", Secret: cfg.MediaSecret},
		"field-team": {Scope: "hazard:read:raw", Secret: cfg.FieldTeamSecret},
		"bnpb-ops":   {Scope: "hazard:read:raw", Secret: cfg.BnpbOpsSecret},
	}
	return &tokenHandler{
		clients:     clients,
		cfg:         cfg,
		redisClient: redisClient,
		kafkaClient: kafkaClient,
		keyId:       "auth-service-key",
		signingKey:  signingKey,
		publicKeyX:  base64.RawURLEncoding.EncodeToString(publicKey),
	}, nil
}

func (h *tokenHandler) IssueToken(c fiber.Ctx) error {
	req := new(TokenRequest)

	err := c.Bind().Body(req)
	if err != nil {
		return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
	}

	switch req.GrantType {
	case "client_credentials":
		if req.RefreshToken != "" {
			return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Refresh token should not be provided for client_credentials grant type")
		}
		if req.ClientID == "" || req.ClientSecret == "" {
			return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Client ID and secret must be provided for client_credentials grant type")
		}
		// check id + secret, issue token
		client, exists := h.clients[req.ClientID]
		if !exists {
			return common.WriteError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "Invalid client")
		}
		if client.Secret != req.ClientSecret {
			return common.WriteError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "Invalid secret")
		}

		// Issue token
		// On success, build a JWT with jti, sid, sub, scope, iss, aud, iat, exp.
		sid, err := uuid.NewV7()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to generate session ID")
		}

		jti, err := uuid.NewV7()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to generate token ID")
		}

		jwtClaims := jwt.MapClaims{
			"sub":   req.ClientID,
			"scope": client.Scope,
			"exp":   time.Now().Add(time.Duration(h.cfg.AccessTokenTTL) * time.Second).Unix(),
			"iat":   time.Now().Unix(),
			"iss":   h.cfg.JWTIssuer,
			"aud":   h.cfg.JWTAudience,
			"sid":   sid.String(),
			"jti":   jti.String(),
		}

		jwtToken := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwtClaims)
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to create JWT token")
		}

		jwtToken.Header["kid"] = h.keyId

		signedToken, err := jwtToken.SignedString(h.signingKey)
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to sign token")
		}

		rawRefreshToken, hashedRefreshToken, err := NewToken()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to generate refresh token")
		}

		// Store session as a Redis hash: rt:{sid} → {hash, jti, sub, scope}
		err = h.redisClient.HSet(c.Context(), "rt:"+sid.String(),
			"hash", hashedRefreshToken,
			"jti", jti.String(),
			"sub", req.ClientID,
			"scope", client.Scope,
		).Err()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to store refresh token in Redis")
		}
		// Set expiration on the hash key
		h.redisClient.Expire(c.Context(), "rt:"+sid.String(), time.Duration(h.cfg.RefreshTokenTTL)*time.Second)

		return c.Status(fiber.StatusOK).JSON(TokenResponse{
			AccessToken:  signedToken,
			RefreshToken: sid.String() + "." + rawRefreshToken,
			TokenType:    "Bearer",
			ExpiresIn:    h.cfg.AccessTokenTTL, // in seconds
			Scope:        client.Scope,
		})

	case "refresh_token":
		if req.ClientID != "" || req.ClientSecret != "" {
			return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Client ID and secret should not be provided for refresh_token grant type")
		}
		if req.RefreshToken == "" {
			return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Refresh token must be provided for refresh_token grant type")
		}

		sid, secret, ok := parseRefreshToken(req.RefreshToken) // split on ".", validate sid is a UUID
		if !ok {
			return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Invalid refresh token format")
		}

		newRawRefreshToken, newHashedRefreshToken, err := NewToken()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to generate new refresh token")
		}

		jti, err := uuid.NewV7()
		if err != nil {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to generate new token ID")
		}

		vals, err := rotate.Run(
			c.Context(), h.redisClient,
			[]string{"rt:" + sid},
			HashSecret(secret), newHashedRefreshToken, jti.String(),
		).Slice()
		if err != nil || len(vals) != 4 {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to rotate refresh token") // Redis problem, not the client's fault
		}

		status, ok1 := vals[0].(int64)
		oldJti, ok2 := vals[1].(string)
		sub, ok3 := vals[2].(string)
		scope, ok4 := vals[3].(string)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Something went wrong.")
		}

		switch status {
		default: // unknown, expired, or already revoked
			return common.WriteError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "Refresh token is invalid, expired, or already revoked.")
		case 1: // rotated
			// Publish old jti to auth-revocations so downstream services revoke it
			record := &kgo.Record{
				Topic: "auth-revocations",
				Value: []byte(oldJti),
			}
			if err := h.kafkaClient.ProduceSync(c.Context(), record).FirstErr(); err != nil {
				return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to publish revocation message")
			}

			// Sign new access token with same sid, new jti, and original sub/scope from Redis
			jwtClaims := jwt.MapClaims{
				"sub":   sub,
				"scope": scope,
				"exp":   time.Now().Add(time.Duration(h.cfg.AccessTokenTTL) * time.Second).Unix(),
				"iat":   time.Now().Unix(),
				"iss":   h.cfg.JWTIssuer,
				"aud":   h.cfg.JWTAudience,
				"sid":   sid,
				"jti":   jti.String(),
			}

			jwtToken := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwtClaims)
			jwtToken.Header["kid"] = h.keyId

			signedToken, err := jwtToken.SignedString(h.signingKey)
			if err != nil {
				return common.WriteError(c, fiber.StatusInternalServerError, "INTERNAL", "Failed to sign new access token")
			}

			return c.Status(fiber.StatusOK).JSON(TokenResponse{
				AccessToken:  signedToken,
				RefreshToken: sid + "." + newRawRefreshToken,
				TokenType:    "Bearer",
				ExpiresIn:    h.cfg.AccessTokenTTL,
				Scope:        scope,
			})

		case -1: // reuse detected
			// The Lua script already DEL'd rt:{sid}, so the session is destroyed.
			// Publish the current (compromised) jti to auth-revocations.
			record := &kgo.Record{
				Topic: "auth-revocations",
				Value: []byte(oldJti),
			}
			_ = h.kafkaClient.ProduceSync(c.Context(), record).FirstErr() // best-effort
			return common.WriteError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "Token reuse detected. Please try logging in again.")
		}

	default:
		// 400
		return common.WriteError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", "Unsupported grant type")
	}
}

func (h *tokenHandler) JWKS(c fiber.Ctx) error {
	// Return the JSON Web Key Set (JWKS)
	// {
	//   "kty": "OKP",
	//   "crv": "Ed25519",
	//   "x": "string",
	//   "kid": "string",
	//   "alg": "EdDSA",
	//   "use": "sig"
	// }
	return c.Status(fiber.StatusOK).JSON(map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "OKP",
				"crv": "Ed25519",
				"alg": "EdDSA",
				"use": "sig",
				"x":   h.publicKeyX,
				"kid": h.keyId,
			},
		},
	})
}

func parseRefreshToken(token string) (sid string, secret string, ok bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	sid = parts[0]
	secret = parts[1]
	return sid, secret, true
}

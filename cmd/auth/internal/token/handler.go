package token

import (
	"time"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	Scope  string
	Secret string // filled from env at startup
}

type tokenHandler struct {
	clients     map[string]Client
	cfg         config.TokenConfig
	redisClient *redis.Client
}

func NewTokenHandler(
	cfg config.TokenConfig,
	redisClient *redis.Client,
) *tokenHandler {
	clients := map[string]Client{
		"media":      {Scope: "hazard:read:summary", Secret: cfg.MediaSecret},
		"field-team": {Scope: "hazard:read:raw", Secret: cfg.FieldTeamSecret},
		"bnpb-ops":   {Scope: "hazard:read:raw", Secret: cfg.BnpbOpsSecret},
	}
	return &tokenHandler{clients: clients, cfg: cfg, redisClient: redisClient}
}

func (h *tokenHandler) IssueToken(c fiber.Ctx) error {
	req := new(TokenRequest)

	err := c.Bind().Body(req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}

	switch req.GrantType {
	case "client_credentials":
		// check id + secret, issue token
		client, exists := h.clients[req.ClientID]
		if !exists {
			return c.Status(fiber.StatusUnauthorized).SendString("Invalid client")
		}
		if client.Secret != req.ClientSecret {
			return c.Status(fiber.StatusUnauthorized).SendString("Invalid secret")
		}

		// Issue token
		// On success, build a JWT with jti, sid, sub, scope, iss, aud, iat, exp.
		sid, err := uuid.NewV7()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate session ID")
		}

		jti, err := uuid.NewV7()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate token ID")
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

		jwtToken, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwtClaims).SignedString([]byte(h.cfg.PrivateKey))
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate token")
		}

		rawRefreshToken, hashedRefreshToken, err := NewToken(sid.String())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate refresh token")
		}

		// Store hashedRefreshToken in Redis with sid as key and set expiration to RefreshTokenTTL
		err = h.redisClient.Set(c.Context(), "rf:"+sid.String(), hashedRefreshToken, time.Duration(h.cfg.RefreshTokenTTL)*time.Second).Err()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to store refresh token")
		}

		return c.Status(fiber.StatusOK).JSON(TokenResponse{
			AccessToken:  jwtToken,
			RefreshToken: sid.String() + "." + rawRefreshToken,
			TokenType:    "Bearer",
			ExpiresIn:    h.cfg.AccessTokenTTL,
			Scope:        client.Scope,
		})

	case "refresh_token":
		// later
	default:
		// 400
		return c.Status(fiber.StatusBadRequest).SendString("Invalid grant type")
	}

	return c.SendString("Token issued successfully")
}

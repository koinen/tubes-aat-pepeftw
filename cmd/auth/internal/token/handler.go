package token

import (
	"time"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
)

type Client struct {
	Scope  string
	Secret string // filled from env at startup
}

type tokenHandler struct {
	clients map[string]Client
	cfg     config.TokenConfig
}

func NewTokenHandler(cfg config.TokenConfig) *tokenHandler {
	clients := map[string]Client{
		"media":      {Scope: "hazard:read:summary", Secret: cfg.MediaSecret},
		"field-team": {Scope: "hazard:read:raw", Secret: cfg.FieldTeamSecret},
		"bnpb-ops":   {Scope: "hazard:read:raw", Secret: cfg.BnpbOpsSecret},
	}
	return &tokenHandler{clients: clients, cfg: cfg}
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
		jwtClaims := jwt.MapClaims{
			"sub":   req.ClientID,
			"scope": client.Scope,
			"exp":   time.Now().Add(time.Duration(h.cfg.AccessTokenTTL) * time.Second).Unix(),
			"iat":   time.Now().Unix(),
			"iss":   h.cfg.JWTIssuer,
			"aud":   h.cfg.JWTAudience,
			// "sid":   "session-id",
			// "jti":   "unique-token-id",
		}

		jwtToken, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwtClaims).SignedString([]byte(h.cfg.PrivateKey))
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate token")
		}
		// put refresh token here???

		// Return access_token, token_type, expires_in, scope.
		return c.JSON(fiber.Map{
			"access_token": jwtToken,
			"token_type":   "Bearer",
			"expires_in":   h.cfg.AccessTokenTTL,
			"scope":        client.Scope,
		})
	case "refresh_token":
		// later
	default:
		// 400
		return c.Status(fiber.StatusBadRequest).SendString("Invalid grant type")
	}

	return c.SendString("Token issued successfully")
}

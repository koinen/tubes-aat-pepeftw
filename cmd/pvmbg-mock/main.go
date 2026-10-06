package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
)

const serviceName = "pvmbg-mock"

type Config struct {
	Port           string
	TokenSecret    string
	AdminKey       string
	DelayMin       time.Duration
	DelayMax       time.Duration
	ReportInterval time.Duration
	SeedCount      int
	SchemaVersion  int
	HangMax        time.Duration
	LogLevel       string
}

func loadConfig() (Config, error) {
	cfg := Config{
		Port:        envOr("PORT", "8080"),
		TokenSecret: os.Getenv("PVMBG_TOKEN_SECRET"),
		AdminKey:    os.Getenv("PVMBG_ADMIN_KEY"),
		LogLevel:    envOr("LOG_LEVEL", "INFO"),
	}
	var err error
	if cfg.DelayMin, err = envDuration("PVMBG_DELAY_MIN", 500*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.DelayMax, err = envDuration("PVMBG_DELAY_MAX", 3000*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.ReportInterval, err = envDuration("PVMBG_REPORT_INTERVAL", 5*time.Second); err != nil {
		return cfg, err
	}
	if cfg.HangMax, err = envDuration("PVMBG_HANG_MAX", 5*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.SeedCount, err = envInt("PVMBG_SEED_COUNT", 25); err != nil {
		return cfg, err
	}
	if cfg.SchemaVersion, err = envInt("PVMBG_SCHEMA_VERSION", 1); err != nil {
		return cfg, err
	}

	switch {
	case cfg.TokenSecret == "":
		return cfg, fmt.Errorf("PVMBG_TOKEN_SECRET wajib diisi")
	case cfg.AdminKey == "":
		return cfg, fmt.Errorf("PVMBG_ADMIN_KEY wajib diisi")
	case cfg.DelayMin < 0 || cfg.DelayMax < cfg.DelayMin:
		return cfg, fmt.Errorf("PVMBG_DELAY_MIN/PVMBG_DELAY_MAX tidak valid")
	case cfg.ReportInterval <= 0 || cfg.ReportInterval > 10*time.Second:
		return cfg, fmt.Errorf("PVMBG_REPORT_INTERVAL harus > 0 dan <= 10s (spesifikasi: minimal 1 laporan per 10 detik)")
	case cfg.HangMax <= 0:
		return cfg, fmt.Errorf("PVMBG_HANG_MAX harus > 0")
	case cfg.SeedCount < 20:
		return cfg, fmt.Errorf("PVMBG_SEED_COUNT minimal 20 (spesifikasi)")
	case cfg.SchemaVersion != 1 && cfg.SchemaVersion != 2:
		return cfg, fmt.Errorf("PVMBG_SCHEMA_VERSION harus 1 atau 2")
	}
	return cfg, nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gen-token" {
		genTokenCmd(os.Args[2:])
		return
	}

	log := newLogger(serviceName, envOr("LOG_LEVEL", "INFO"))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("config_invalid", "error", err.Error())
		os.Exit(1)
	}

	store := &Store{}
	gen := seed(store, cfg.SeedCount)
	state := newState(cfg.SchemaVersion)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go runGenerator(ctx, store, gen, state, cfg.ReportInterval, log)

	app := newApp(ctx, cfg, store, state, log)
	log.Info("service_started", "port", cfg.Port, "seed_reports", store.Count(),
		"schema_version", cfg.SchemaVersion, "report_interval", cfg.ReportInterval.String(),
		"delay_min", cfg.DelayMin.String(), "delay_max", cfg.DelayMax.String())

	if err := app.Listen(":"+cfg.Port, fiber.ListenConfig{
		DisableStartupMessage: true,
		GracefulContext:       ctx,
		ShutdownTimeout:       5 * time.Second,
	}); err != nil {
		log.Error("server_stopped", "error", err.Error())
		os.Exit(1)
	}
	log.Info("service_stopped")
}

func newApp(ctx context.Context, cfg Config, store *Store, state *State, log *slog.Logger) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:      serviceName,
		ReadTimeout:  30 * time.Second,
		ErrorHandler: errorHandler,
	})
	app.Use(correlationMiddleware(log))

	app.Get("/health", func(c fiber.Ctx) error {
		if enabled, _, _ := state.Outage(); enabled {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"status": "outage", "service": serviceName})
		}
		return c.JSON(fiber.Map{"status": "ok", "service": serviceName})
	})

	app.Get("/volcanic-reports",
		outageMiddleware(ctx, state, cfg.HangMax, log),
		delayMiddleware(cfg.DelayMin, cfg.DelayMax),
		authMiddleware(cfg.TokenSecret),
		func(c fiber.Ctx) error {
			since, err := parseSince(c)
			if err != nil {
				return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			}
			return c.JSON(fiber.Map{
				"reports":      store.Since(since),
				"generated_at": time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
			})
		})

	admin := adminMiddleware(cfg.AdminKey)
	app.Post("/admin/schema-version", admin, func(c fiber.Ctx) error {
		var body struct {
			Version *int `json:"version"`
		}
		if err := json.Unmarshal(c.Body(), &body); err != nil || body.Version == nil ||
			(*body.Version != 1 && *body.Version != 2) {
			return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", `body harus {"version": 1} atau {"version": 2}`)
		}
		state.SetSchemaVersion(*body.Version)
		log.Info("schema_version_changed", "correlation_id", correlationID(c), "schema_version", *body.Version)
		return c.JSON(state.Snapshot())
	})
	app.Post("/admin/outage", admin, func(c fiber.Ctx) error {
		var body struct {
			Enabled *bool  `json:"enabled"`
			Mode    string `json:"mode"`
		}
		if err := json.Unmarshal(c.Body(), &body); err != nil || body.Enabled == nil {
			return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", `body harus {"enabled": true|false, "mode": "hang"|"error"}`)
		}
		if body.Mode == "" {
			body.Mode = outageModeHang
		}
		if body.Mode != outageModeHang && body.Mode != outageModeError {
			return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", `mode harus "hang" atau "error"`)
		}
		state.SetOutage(*body.Enabled, body.Mode)
		log.Info("outage_changed", "correlation_id", correlationID(c), "enabled", *body.Enabled, "mode", body.Mode)
		return c.JSON(state.Snapshot())
	})
	app.Get("/admin/state", admin, func(c fiber.Ctx) error {
		return c.JSON(state.Snapshot())
	})
	return app
}

func outageMiddleware(ctx context.Context, state *State, hangMax time.Duration, log *slog.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		enabled, mode, recovered := state.Outage()
		if !enabled {
			return c.Next()
		}
		if mode == outageModeHang {
			log.Debug("outage_hang_start", "correlation_id", correlationID(c))
			if waitWhileHanging(ctx, recovered, hangMax) {
				return c.Next()
			}
		}
		c.Set(fiber.HeaderRetryAfter, "5")
		return sendError(c, fiber.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "PVMBG sedang tidak tersedia (outage)")
	}
}

func delayMiddleware(min, max time.Duration) fiber.Handler {
	return func(c fiber.Ctx) error {
		d := min
		if max > min {
			d += rand.N(max - min + 1)
		}
		time.Sleep(d)
		return c.Next()
	}
}

func signToken(secret, clientID string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(clientID))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func makeToken(secret, clientID string) string {
	return "pvmbg." + clientID + "." + signToken(secret, clientID)
}

func verifyToken(secret, token string) (clientID string, ok bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "pvmbg" || parts[1] == "" {
		return "", false
	}
	want := signToken(secret, parts[1])
	return parts[1], hmac.Equal([]byte(parts[2]), []byte(want))
}

func authMiddleware(secret string) fiber.Handler {
	return func(c fiber.Ctx) error {
		h := c.Get("Authorization")
		token := ""
		if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
			token = strings.TrimSpace(h[7:])
		}
		if token != "" {
			if clientID, ok := verifyToken(secret, token); ok {
				c.Locals("client_id", clientID)
				return c.Next()
			}
		}
		switch {
		case token == "" && c.Get("X-BMKG-Key") != "":
			return sendError(c, fiber.StatusForbidden, "FORBIDDEN",
				"kredensial BMKG tidak berlaku untuk PVMBG, gunakan Authorization: Bearer <pvmbg-token>")
		case token == "":
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "header Authorization: Bearer <pvmbg-token> wajib diisi")
		case !strings.HasPrefix(token, "pvmbg."):
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "format token harus pvmbg.<client_id>.<hmac>")
		default:
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "tanda tangan token PVMBG tidak valid")
		}
	}
}

func adminMiddleware(adminKey string) fiber.Handler {
	want := []byte(adminKey)
	return func(c fiber.Ctx) error {
		if subtle.ConstantTimeCompare([]byte(c.Get("X-Admin-Key")), want) != 1 {
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "header X-Admin-Key tidak ada atau salah")
		}
		return c.Next()
	}
}

func genTokenCmd(args []string) {
	secret := os.Getenv("PVMBG_TOKEN_SECRET")
	if len(args) != 1 || secret == "" {
		fmt.Fprintln(os.Stderr, "penggunaan: PVMBG_TOKEN_SECRET=<secret> pvmbg-mock gen-token <client_id>")
		os.Exit(2)
	}
	if strings.Contains(args[0], ".") {
		fmt.Fprintln(os.Stderr, "client_id tidak boleh mengandung titik")
		os.Exit(2)
	}
	fmt.Println(makeToken(secret, args[0]))
}

func parseSince(c fiber.Ctx) (time.Time, error) {
	raw := c.Query("since")
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parameter since harus RFC 3339, contoh 2026-10-01T07:00:00Z")
	}
	return t.UTC(), nil
}

func errorHandler(c fiber.Ctx, err error) error {
	status := fiber.StatusInternalServerError
	code := "INTERNAL"
	msg := "terjadi kesalahan internal"
	if fe, ok := err.(*fiber.Error); ok {
		status = fe.Code
		msg = fe.Message
		switch {
		case status == fiber.StatusNotFound:
			code = "NOT_FOUND"
		case status < 500:
			code = "VALIDATION_ERROR"
		}
	}
	return c.Status(status).JSON(errorBody(c, code, msg))
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s harus format durasi Go (contoh 500ms): %w", key, err)
	}
	return d, nil
}

func envInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s harus bilangan bulat: %w", key, err)
	}
	return n, nil
}
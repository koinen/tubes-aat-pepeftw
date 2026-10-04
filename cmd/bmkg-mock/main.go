package main

import (
	"context"
	"crypto/subtle"
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

const serviceName = "bmkg-mock"

type Config struct {
	Port          string
	APIKey        string
	DelayMin      time.Duration
	DelayMax      time.Duration
	EventInterval time.Duration
	SeedCount     int
	TsunamiRate   float64
	LogLevel      string
}

func loadConfig() (Config, error) {
	cfg := Config{
		Port:     envOr("PORT", "8080"),
		APIKey:   os.Getenv("BMKG_API_KEY"),
		LogLevel: envOr("LOG_LEVEL", "INFO"),
	}
	var err error
	if cfg.DelayMin, err = envDuration("BMKG_DELAY_MIN", 50*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.DelayMax, err = envDuration("BMKG_DELAY_MAX", 150*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.EventInterval, err = envDuration("BMKG_EVENT_INTERVAL", 5*time.Second); err != nil {
		return cfg, err
	}
	if cfg.SeedCount, err = envInt("BMKG_SEED_COUNT", 25); err != nil {
		return cfg, err
	}
	if cfg.TsunamiRate, err = envFloat("BMKG_TSUNAMI_RATE", 0.15); err != nil {
		return cfg, err
	}

	switch {
	case cfg.APIKey == "":
		return cfg, fmt.Errorf("BMKG_API_KEY wajib diisi")
	case cfg.DelayMin < 0 || cfg.DelayMax < cfg.DelayMin:
		return cfg, fmt.Errorf("BMKG_DELAY_MIN/BMKG_DELAY_MAX tidak valid")
	case cfg.EventInterval <= 0 || cfg.EventInterval > 10*time.Second:
		return cfg, fmt.Errorf("BMKG_EVENT_INTERVAL harus > 0 dan <= 10s (spesifikasi: minimal 1 event per 10 detik)")
	case cfg.SeedCount < 20:
		return cfg, fmt.Errorf("BMKG_SEED_COUNT minimal 20 (spesifikasi)")
	case cfg.TsunamiRate < 0 || cfg.TsunamiRate > 1:
		return cfg, fmt.Errorf("BMKG_TSUNAMI_RATE harus di antara 0 dan 1")
	}
	return cfg, nil
}

func main() {
	log := newLogger(serviceName, envOr("LOG_LEVEL", "INFO"))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("config_invalid", "error", err.Error())
		os.Exit(1)
	}

	store := &Store{}
	seed(store, cfg.SeedCount)
	nEv, nW := store.Counts()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go runGenerator(ctx, store, cfg.EventInterval, cfg.TsunamiRate, log)

	app := newApp(cfg, store, log)
	log.Info("service_started", "port", cfg.Port, "seed_events", nEv, "seed_warnings", nW,
		"event_interval", cfg.EventInterval.String(),
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

func newApp(cfg Config, store *Store, log *slog.Logger) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:      serviceName,
		ReadTimeout:  30 * time.Second,
		ErrorHandler: errorHandler,
	})
	app.Use(correlationMiddleware(log))

	app.Get("/health", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "service": serviceName})
	})

	delay := delayMiddleware(cfg.DelayMin, cfg.DelayMax)
	auth := authMiddleware(cfg.APIKey)
	app.Get("/seismic-events", delay, auth, func(c fiber.Ctx) error {
		since, err := parseSince(c)
		if err != nil {
			return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		}
		return c.JSON(store.EventsSince(since))
	})
	app.Get("/tsunami-warnings", delay, auth, func(c fiber.Ctx) error {
		since, err := parseSince(c)
		if err != nil {
			return sendError(c, fiber.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		}
		return c.JSON(store.WarningsSince(since))
	})
	return app
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

func authMiddleware(apiKey string) fiber.Handler {
	want := []byte(apiKey)
	return func(c fiber.Ctx) error {
		key := c.Get("X-BMKG-Key")
		bearer := bearerToken(c.Get("Authorization"))
		switch {
		case key != "" && subtle.ConstantTimeCompare([]byte(key), want) == 1:
			return c.Next()
		case isPVMBGToken(key) || isPVMBGToken(bearer):
			return sendError(c, fiber.StatusForbidden, "FORBIDDEN",
				"kredensial PVMBG tidak berlaku untuk BMKG, gunakan header X-BMKG-Key")
		case key == "":
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "header X-BMKG-Key wajib diisi")
		default:
			return sendError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "X-BMKG-Key tidak valid")
		}
	}
}

func bearerToken(h string) string {
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func isPVMBGToken(s string) bool {
	return strings.HasPrefix(s, "pvmbg.") && strings.Count(s, ".") == 2
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
		return 0, fmt.Errorf("%s harus format durasi Go (contoh 150ms): %w", key, err)
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

func envFloat(key string, def float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s harus angka desimal: %w", key, err)
	}
	return f, nil
}
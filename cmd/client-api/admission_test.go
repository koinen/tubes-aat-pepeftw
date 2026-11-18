package main

import (
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
	"tubes-aat-pepeftw/cmd/client-api/internal/ratelimit"
)

func TestMediaSaturationPreservesTimLapanganAdmission(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	api := newAdmissionTestAPI(t, 2, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") == "1" {
			close(entered)
			<-release
		}
		_, _ = io.WriteString(w, `{"data":[]}`)
	}, func(c fiber.Ctx) error { return c.Next() })

	holdHazardsRequest(t, api, "/hazards?limit=1", "media", "hazard:read:summary", entered, release)

	status, body, header := api.do(http.MethodGet, "/hazards", "media", "hazard:read:summary")
	if status != http.StatusServiceUnavailable || errorCode(body) != "overloaded" || header.Get("Retry-After") != "2" {
		t.Errorf("second Media request = %d, %v, Retry-After %q; want 503 overloaded with Retry-After: 2",
			status, body, header.Get("Retry-After"))
	}
	status, _, _ = api.do(http.MethodGet, "/hazards", "other-client", "hazard:read:raw")
	if status != http.StatusServiceUnavailable {
		t.Errorf("unrecognized subject with raw scope = %d; want 503, not reserved admission", status)
	}
	status, body, _ = api.do(http.MethodGet, "/hazards", "field-team", fieldTeamScopes)
	if status != http.StatusOK {
		t.Errorf("Tim Lapangan request = %d, %v; want 200 while Media slot is occupied", status, body)
	}
}

func TestOverloadTakesPrecedenceOverRateLimit(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	redisClient := redis.NewClient(&redis.Options{Addr: "unused"})
	t.Cleanup(func() { _ = redisClient.Close() })
	replies := &rateLimitRedisReplies{}
	redisClient.AddHook(replies)
	limiter := ratelimit.New(redisClient, config.RateLimitConfig{Max: 1, WindowSeconds: 60})
	api := newAdmissionTestAPI(t, 2, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") == "1" {
			close(entered)
			<-release
		}
		_, _ = io.WriteString(w, `{"data":[]}`)
	}, limiter.Middleware)
	releaseMedia := holdHazardsRequest(t, api, "/hazards?limit=1", "media", "hazard:read:summary", entered, release)
	replies.exceeded.Store(true)

	status, body, _ := api.do(http.MethodGet, "/hazards", "media", "hazard:read:summary")
	if status != http.StatusServiceUnavailable || errorCode(body) != "overloaded" {
		t.Errorf("at capacity and over quota = %d, %v; want 503 overloaded", status, body)
	}
	if calls := replies.calls.Load(); calls != 1 {
		t.Errorf("Redis calls = %d; want just the one held request's quota check", calls)
	}

	releaseMedia()
	status, body, header := api.do(http.MethodGet, "/hazards", "media", "hazard:read:summary")
	if status != http.StatusTooManyRequests || errorCode(body) != "RATE_LIMITED" || header.Get("Retry-After") != "60" {
		t.Errorf("only over quota = %d, %v, Retry-After %q; want 429 RATE_LIMITED with Retry-After: 60",
			status, body, header.Get("Retry-After"))
	}

	replies.exceeded.Store(false)
	status, body, _ = api.do(http.MethodGet, "/hazards", "media", "hazard:read:summary")
	if status != http.StatusOK {
		t.Errorf("after rate limit clears = %d, %v; want 200 (the 429 did not hold a slot)", status, body)
	}
}

// rateLimitRedisReplies stands in for Redis, not the HTTP rate limiter.
type rateLimitRedisReplies struct {
	exceeded atomic.Bool
	calls    atomic.Int32
}

func (*rateLimitRedisReplies) DialHook(next redis.DialHook) redis.DialHook { return next }

func (*rateLimitRedisReplies) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (r *rateLimitRedisReplies) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if reply, ok := cmd.(*redis.Cmd); ok && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
			r.calls.Add(1)
			if r.exceeded.Load() {
				reply.SetVal([]any{int64(0), int64(60)})
			} else {
				reply.SetVal([]any{int64(1), int64(0)})
			}
			return nil
		}
		return next(ctx, cmd)
	}
}

func TestPriorityBorrowsAndReleasesMediaCapacity(t *testing.T) {
	fieldEntered, fieldRelease := make(chan struct{}), make(chan struct{})
	opsEntered, opsRelease := make(chan struct{}), make(chan struct{})
	api := newAdmissionTestAPI(t, 2, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("limit") {
		case "1":
			close(fieldEntered)
			<-fieldRelease
		case "2":
			close(opsEntered)
			<-opsRelease
		}
		_, _ = io.WriteString(w, `{"data":[]}`)
	}, func(c fiber.Ctx) error { return c.Next() })

	holdHazardsRequest(t, api, "/hazards?limit=1", "field-team", fieldTeamScopes, fieldEntered, fieldRelease)
	releaseOps := holdHazardsRequest(t, api, "/hazards?limit=2", "bnpb-ops", opsScopes, opsEntered, opsRelease)

	for _, subject := range []string{"media", "field-team", "bnpb-ops"} {
		status, body, header := api.do(http.MethodGet, "/hazards", subject, "hazard:read:raw")
		if status != http.StatusServiceUnavailable || errorCode(body) != "overloaded" || header.Get("Retry-After") != "2" {
			t.Errorf("%s at capacity = %d, %v, Retry-After %q; want 503 overloaded with Retry-After: 2",
				subject, status, body, header.Get("Retry-After"))
		}
	}
	status, body, _ := api.do(http.MethodGet, "/hazards", "", "")
	if status != http.StatusUnauthorized {
		t.Errorf("unauthenticated request at capacity = %d, %v; want 401", status, body)
	}

	releaseOps()
	status, body, _ = api.do(http.MethodGet, "/hazards", "media", "hazard:read:summary")
	if status != http.StatusOK {
		t.Errorf("Media after borrowed slot release = %d, %v; want 200", status, body)
	}
}

func holdHazardsRequest(t *testing.T, api *testAPI, target, subject, scope string, entered <-chan struct{}, release chan<- struct{}) func() {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer "+api.token(subject, scope))
	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := api.call(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		resp.Body.Close()
		done <- result{status: resp.StatusCode}
	}()
	stop := sync.OnceFunc(func() {
		close(release)
		r := <-done
		if r.err != nil || r.status != http.StatusOK {
			t.Errorf("held %s request = %d, %v; want 200", subject, r.status, r.err)
		}
	})
	t.Cleanup(stop)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not reach the aggregator", subject)
	}
	return stop
}

func newAdmissionTestAPI(t *testing.T, maxRequests int, upstream http.HandlerFunc, rateLimit fiber.Handler) *testAPI {
	t.Helper()
	agg := httptest.NewServer(upstream)
	t.Cleanup(agg.Close)
	pub, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier := auth.NewVerifier(&auth.JWKS{PublicKey: pub, Kid: testKid}, testIssuer, testAudience, nil)
	app := newApp(appDeps{
		Agg:     aggregator.NewClient(config.AggregatorConfig{BaseURL: agg.URL}, log),
		Protect: protectedMiddleware(maxRequests, verifier.Middleware, rateLimit),
		Log:     log,
	})
	return &testAPI{t: t, key: key, call: func(req *http.Request) (*http.Response, error) {
		return app.Test(req, fiber.TestConfig{Timeout: 2 * time.Second, FailOnTimeout: true})
	}}
}

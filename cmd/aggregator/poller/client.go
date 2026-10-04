package poller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const maxBodyBytes = 10 << 20

type fetcher struct {
	source  model.Source
	baseURL string
	client  *http.Client
	timeout time.Duration
	auth    func(*http.Request)
	log     *slog.Logger
	now     func() time.Time
}

func newFetcher(source model.Source, baseURL string, timeout time.Duration, auth func(*http.Request), log *slog.Logger, now func() time.Time) *fetcher {
	return &fetcher{
		source:  source,
		baseURL: baseURL,
		client:  &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		timeout: timeout,
		auth:    auth,
		log:     log,
		now:     now,
	}
}

func (f *fetcher) get(ctx context.Context, path string, since *time.Time) ([]byte, error) {
	u := f.baseURL + path
	if since != nil {
		u += "?since=" + url.QueryEscape(since.UTC().Format(time.RFC3339))
	}

	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	cid := correlation.From(ctx)
	if cid != "" {
		req.Header.Set("X-Correlation-ID", cid)
	}
	f.auth(req)

	start := f.now()
	resp, err := f.client.Do(req)
	latency := f.now().Sub(start).Milliseconds()
	if err != nil {
		f.log.Warn("outbound_call", "correlation_id", cid, "target", string(f.source), "method", http.MethodGet,
			"path", path, "status", 0, "latency_ms", latency, "error", err.Error())
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	level := slog.LevelInfo
	if resp.StatusCode >= 300 || readErr != nil {
		level = slog.LevelWarn
	}
	f.log.Log(ctx, level, "outbound_call", "correlation_id", cid, "target", string(f.source), "method", http.MethodGet,
		"path", path, "status", resp.StatusCode, "latency_ms", latency)

	if readErr != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", path, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return body, nil
}

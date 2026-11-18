package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"tubes-aat-pepeftw/cmd/client-api/internal/config"

	"tubes-aat-pepeftw/internal/telemetry"
)

type Client struct {
	baseURL     string
	internalKey string
	opsToken    string
	httpClient  *http.Client
	log         *slog.Logger
}

const maxIdleConns = 256

func NewClient(cfg config.AggregatorConfig, log *slog.Logger) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxIdleConns
	transport.MaxIdleConnsPerHost = maxIdleConns

	return &Client{
		baseURL:     cfg.BaseURL,
		internalKey: cfg.InternalKey,
		opsToken:    cfg.OpsToken,
		httpClient:  &http.Client{Timeout: 5 * time.Second, Transport: telemetry.Transport(transport)},
		log:         log,
	}
}

func (c *Client) Ping() error {
	resp, err := c.do(context.Background(), http.MethodGet, "/health", "", c.internalKeyHeader())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aggregator health returned status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, path, correlationID string) (map[string]any, int, error) {
	return c.call(ctx, http.MethodGet, path, correlationID, c.internalKeyHeader())
}

// OpsConfigured reports whether client-api holds OPS_TOKEN, which the ops API needs.
func (c *Client) OpsConfigured() bool {
	return c.opsToken != ""
}

// Ops calls the aggregator's ops API with OPS_TOKEN. requestedBy, if set, is sent as
// X-Requested-By; the aggregator trusts it because only client-api holds the token.
func (c *Client) Ops(ctx context.Context, method, path, correlationID, requestedBy string) (map[string]any, int, error) {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+c.opsToken)
	if requestedBy != "" {
		h.Set("X-Requested-By", requestedBy)
	}
	return c.call(ctx, method, path, correlationID, h)
}

func (c *Client) internalKeyHeader() http.Header {
	h := http.Header{}
	h.Set("X-Internal-Key", c.internalKey)
	return h
}

func (c *Client) call(ctx context.Context, method, path, correlationID string, header http.Header) (map[string]any, int, error) {
	resp, err := c.do(ctx, method, path, correlationID, header)
	if err != nil {
		return nil, 0, fmt.Errorf("aggregator request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read aggregator response body: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decode aggregator response JSON: %w", err)
	}

	return result, resp.StatusCode, nil
}

func (c *Client) do(ctx context.Context, method, path, correlationID string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build aggregator request: %w", err)
	}
	req.Header = header
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	attrs := []any{
		"correlation_id", correlationID, "target", "aggregator", "method", method,
		"path", path, "latency_ms", time.Since(start).Milliseconds(),
	}
	if err != nil {
		c.log.Warn("outbound_call", append(attrs, "status", 0, "error", err.Error())...)
		return nil, err
	}

	level := slog.LevelInfo
	if resp.StatusCode >= 300 {
		level = slog.LevelWarn
	}
	c.log.Log(context.Background(), level, "outbound_call", append(attrs, "status", resp.StatusCode)...)
	return resp, nil
}

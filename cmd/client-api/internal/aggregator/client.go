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
)

type Client struct {
	baseURL     string
	internalKey string
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
		httpClient:  &http.Client{Timeout: 5 * time.Second, Transport: transport},
		log:         log,
	}
}

func (c *Client) Ping() error {
	resp, err := c.do("/health", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aggregator health returned status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Get(path, correlationID string) (map[string]any, int, error) {
	resp, err := c.do(path, correlationID)
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

func (c *Client) do(path, correlationID string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build aggregator request: %w", err)
	}
	req.Header.Set("X-Internal-Key", c.internalKey)
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	attrs := []any{
		"correlation_id", correlationID, "target", "aggregator", "method", http.MethodGet,
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

package aggregator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

type Client struct {
	baseURL     string
	internalKey string
	httpClient  *http.Client
}

func NewClient(cfg config.AggregatorConfig) *Client {
	return &Client{
		baseURL:     cfg.BaseURL,
		internalKey: cfg.InternalKey,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
	}
}

// Ping checks if the aggregator is reachable.
// Uses the internal api key to communicate with the aggregator.
func (c *Client) Ping() error {
	req, err := http.NewRequest("GET", c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Internal-Key", c.internalKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aggregator health returned status %d", resp.StatusCode)
	}
	return nil
}

// Get makes a GET request to the aggregator internal API using X-Internal-Key.
// Passes X-Correlation-ID for end-to-end tracing.
func (c *Client) Get(path, correlationID string) (map[string]any, int, error) {
	req, err := http.NewRequest("GET", c.baseURL+path, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build aggregator request: %w", err)
	}
	req.Header.Set("X-Internal-Key", c.internalKey)
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}

	resp, err := c.httpClient.Do(req)
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

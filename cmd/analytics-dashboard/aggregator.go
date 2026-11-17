package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"tubes-aat-pepeftw/internal/telemetry"
)

const (
	headerInternalKey   = "X-Internal-Key"
	headerCorrelationID = "X-Correlation-ID"
	maxBody             = 4 << 20
)

type aggregatorClient struct {
	base string
	key  string
	http *http.Client
}

func newAggregatorClient(cfg config) *aggregatorClient {
	return &aggregatorClient{base: cfg.aggregatorURL, key: cfg.internalKey, http: &http.Client{Timeout: cfg.timeout, Transport: telemetry.Transport(nil)}}
}

func (a *aggregatorClient) analytics(ctx context.Context, query url.Values, correlationID string) (int, []byte, error) {
	return a.get(ctx, "/internal/analytics?"+query.Encode(), correlationID)
}

func (a *aggregatorClient) health(ctx context.Context) error {
	status, _, err := a.get(ctx, "/health", uuid.NewString())
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("aggregator health returned %d", status)
	}
	return nil
}

func (a *aggregatorClient) get(ctx context.Context, path, correlationID string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set(headerInternalKey, a.key)
	req.Header.Set(headerCorrelationID, correlationID)
	resp, err := a.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	return resp.StatusCode, body, err
}

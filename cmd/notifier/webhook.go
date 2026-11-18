package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var webhookClient = &http.Client{
	Timeout: 2 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func (n *notifier) deliver(ctx context.Context, msg hazardMessage, attrs []any) error {
	// Keep the entire source payload, including attributes unknown to the notifier.
	body := make(map[string]json.RawMessage)
	if err := json.Unmarshal(msg.rawPayload, &body); err != nil {
		return fmt.Errorf("decode webhook payload: %w", err)
	}
	body["hazard_id"], _ = json.Marshal(msg.HazardID)
	body["event_id"], _ = json.Marshal(msg.EventID)
	body["emitted_at"], _ = json.Marshal(msg.EmittedAt)
	value, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode webhook payload: %w", err)
	}

	for delay := 100 * time.Millisecond; ; delay = min(delay*2, 2*time.Second) {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := postAlert(ctx, n.webhookURL, value)
		if err == nil {
			return nil
		}
		n.log.Warn("webhook_delivery_failed", append(attrs, "error", err.Error(), "retry_in", delay)...)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func postAlert(ctx context.Context, url string, value []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(value))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := webhookClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

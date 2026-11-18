package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBatchCommitsOnlyAfterWebhookDelivery(t *testing.T) {
	for _, acknowledge := range []bool{true, false} {
		name := "acknowledged"
		if !acknowledge {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			seen, log := newNotifierTestDependencies(t)
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			released := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				started <- struct{}{}
				select {
				case <-release:
					w.WriteHeader(http.StatusNoContent)
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer func() {
				if !released {
					close(release)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			committed := make(chan struct{}, 1)
			done := make(chan error, 1)
			n := newNotifier(seen, log, server.URL)
			go func() {
				done <- processBatch(ctx, []*kgo.Record{{Value: []byte(coastalEvent)}}, n, nil, log,
					func(context.Context) error { committed <- struct{}{}; return nil })
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("webhook did not start")
			}
			if len(committed) != 0 {
				t.Fatal("commit attempted while delivery was pending")
			}
			if acknowledge {
				close(release)
				released = true
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if acknowledge && (err != nil || len(committed) != 1) {
					t.Fatalf("acknowledged batch: err=%v, commits=%d", err, len(committed))
				}
				if !acknowledge && (!errors.Is(err, context.Canceled) || len(committed) != 0 || seen.Has("hz-1")) {
					t.Fatalf("canceled batch: err=%v, commits=%d, seen=%v", err, len(committed), seen.Has("hz-1"))
				}
			case <-time.After(3 * time.Second):
				t.Fatal("batch did not finish")
			}
		})
	}
}

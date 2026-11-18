package satellite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"
)

const maxBackoff = 15 * time.Second

type BatchStats struct {
	LastOffset int64
	Detections int
	Duplicates int
	Late       int
	Updates    int
	Retracts   int
	Orphans    int
	Gaps       int
}

type Deps struct {
	LastOffset func(ctx context.Context) (int64, error)
	// Park stores an event that cannot be decoded as an unprocessed message (inserted=false if already stored).
	Park    func(ctx context.Context, m model.UnprocessedMessage) (inserted bool, err error)
	Apply   func(ctx context.Context, events []Event, lateness time.Duration, receivedAt time.Time) (BatchStats, error)
	Log     *slog.Logger
	Client  *http.Client
	Now     func() time.Time
	Backoff time.Duration
}

type Consumer struct {
	cfg Config
	d   Deps
}

func New(cfg Config, d Deps) *Consumer {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Backoff <= 0 {
		d.Backoff = time.Second
	}
	if d.Client == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.ResponseHeaderTimeout = 10 * time.Second
		d.Client = &http.Client{Transport: tr}
	}
	return &Consumer{cfg: cfg, d: d}
}

func (c *Consumer) Run(ctx context.Context) {
	delay := c.d.Backoff
	for ctx.Err() == nil {
		progressed, err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, fence.ErrStaleEpoch) {
			c.d.Log.Error("satellite_consumer_fenced", "error", err.Error())
			return
		}
		c.d.Log.Warn("satellite_stream_ended", "error", err.Error(), "retry_in", delay.String())

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if progressed {
			delay = c.d.Backoff
		} else if delay *= 2; delay > maxBackoff {
			delay = maxBackoff
		}
	}
}

func (c *Consumer) session(ctx context.Context) (progressed bool, err error) {
	last, err := c.d.LastOffset(ctx)
	if err != nil {
		return false, fmt.Errorf("read offset: %w", err)
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	url := strings.TrimRight(c.cfg.URL, "/") + "/v1/stream?from=" + strconv.FormatInt(last+1, 10)
	req, err := http.NewRequestWithContext(sctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Accept", "text/event-stream")
	if last >= 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(last, 10))
	}

	resp, err := c.d.Client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, fmt.Errorf("feed status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	c.d.Log.Info("satellite_stream_connected", "from", last+1)

	idle := time.AfterFunc(c.cfg.IdleTimeout, cancel)
	defer idle.Stop()
	body := &idleReader{r: resp.Body, timer: idle, d: c.cfg.IdleTimeout}

	events := make(chan Event, c.cfg.BatchSize*2)
	readErr := make(chan error, 1)
	go func() {
		defer close(events)
		sampled := 0
		readErr <- ReadSSE(body, func(raw RawEvent) error {
			ev, err := decodeChecked(raw)
			if err != nil {
				if ev, err = c.park(sctx, raw, err); err != nil || ev.Offset < 0 {
					return err
				}
			}
			if ev.Kind == KindHeartbeat {
				if ev.Offset < 0 {
					if n, err := strconv.ParseInt(raw.ID, 10, 64); err == nil {
						ev.Offset = n
					}
				}
				if sampled < 3 {
					sampled++
					c.d.Log.Info("satellite_heartbeat_sample", "id", raw.ID, "data", string(raw.Data), "offset", ev.Offset)
				}
				if ev.Offset < 0 {
					return nil
				}
			}
			select {
			case events <- ev:
				return nil
			case <-sctx.Done():
				return sctx.Err()
			}
		})
	}()

	batch := make([]Event, 0, c.cfg.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		res, err := c.d.Apply(ctx, batch, c.cfg.Lateness, c.d.Now())
		if err != nil {
			return fmt.Errorf("apply batch: %w", err)
		}
		batch = batch[:0]
		progressed = true
		c.d.Log.Info("satellite_batch", "last_offset", res.LastOffset, "detections", res.Detections,
			"duplicates", res.Duplicates, "late", res.Late, "updates", res.Updates,
			"retracts", res.Retracts, "orphans", res.Orphans, "gaps", res.Gaps)
		if res.Gaps > 0 {
			c.d.Log.Warn("satellite_offset_gap", "gaps", res.Gaps)
		}
		return nil
	}

	prev, skips := last, 0
	tick := time.NewTicker(c.cfg.BatchWait)
	defer tick.Stop()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				if err := flush(); err != nil {
					return progressed, err
				}
				if err := <-readErr; err != nil {
					return progressed, err
				}
				return progressed, io.EOF
			}

			if ev.Offset != prev+1 && skips < 5 {
				skips++
				c.d.Log.Warn("satellite_feed_skip", "after", prev, "got", ev.Offset, "kind", string(ev.Kind))
			}
			if ev.Offset > prev {
				prev = ev.Offset
			}

			batch = append(batch, ev)

			if len(batch) >= c.cfg.BatchSize {
				if err := flush(); err != nil {
					return progressed, err
				}
			}
		case <-tick.C:
			if err := flush(); err != nil {
				return progressed, err
			}
		case <-ctx.Done():
			return progressed, ctx.Err()
		}
	}
}

// idleReader mengatur ulang timer tiap ada data masuk, supaya koneksi yang diam terlalu lama diputus.
type idleReader struct {
	r     io.Reader
	timer *time.Timer
	d     time.Duration
}

func (i *idleReader) Read(p []byte) (int, error) {
	n, err := i.r.Read(p)
	if n > 0 {
		i.timer.Reset(i.d)
	}
	return n, err
}

func decodeChecked(raw RawEvent) (Event, error) {
	if err := unprocessed.Check(raw.Data); err != nil {
		return Event{}, err
	}
	return Decode(raw.Type, raw.Data)
}

// park stores an undecodable event and returns a KindParked placeholder that keeps its offset,
// so the stored offset moves past it without counting a gap. If the SSE id is not a number the
// placeholder has offset -1 and is not sent; the event is still in the DLQ. If the event cannot be stored,
// the error ends the session and the next one resumes from the last applied offset.
func (c *Consumer) park(ctx context.Context, raw RawEvent, cause error) (Event, error) {
	m := unprocessed.Build(model.SourceSatelit, strings.TrimSpace(raw.Type), raw.Data, cause, c.d.Now())
	inserted, err := c.d.Park(ctx, m)
	if err != nil {
		return Event{}, fmt.Errorf("park event %s: %w", raw.ID, err)
	}
	if inserted {
		unprocessed.Written(m)
		c.d.Log.Warn("message_unprocessed", "source", string(m.Source), "endpoint", m.Endpoint, "id", raw.ID,
			"message_id", m.MessageID, "status", string(m.Status), "reason", m.Reason)
	}
	offset, perr := strconv.ParseInt(raw.ID, 10, 64)
	if perr != nil {
		offset = -1
	}
	return Event{Offset: offset, Kind: KindParked}, nil
}

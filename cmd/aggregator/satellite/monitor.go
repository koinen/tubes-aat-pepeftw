package satellite

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Progress struct {
	Applied int64 
	Late    int64 
}

type ProgressSource interface {
	Progress(ctx context.Context) (Progress, error)
}

type Lag struct {
	Applied    int64
	Head       int64 
	LagOffsets int64
	Rate       float64 
	ETA        float64 
	Speed      float64 
	Late       int64
}

type LagObserver interface{ LagUpdated(Lag) }

type session struct {
	Speed        float64 `json:"speed"`
	LatestOffset int64   `json:"latest_offset"`
}

type Monitor struct {
	cfg      Config
	src      ProgressSource
	obs      LagObserver
	client   *http.Client
	log      *slog.Logger
	interval time.Duration
	now      func() time.Time

	prevApplied int64
	prevAt      time.Time
	failing     bool
}

func NewMonitor(cfg Config, src ProgressSource, obs LagObserver, log *slog.Logger) *Monitor {
	if log == nil {
		log = slog.Default()
	}
	return &Monitor{cfg: cfg, src: src, obs: obs, log: log, interval: 5 * time.Second,
		client: &http.Client{Timeout: 5 * time.Second}, now: time.Now}
}

func (m *Monitor) Run(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		if err := m.Tick(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			if !m.failing {
				m.log.Warn("satellite_monitor_failed", "err", err)
			}
			m.failing = true
		} else {
			m.failing = false
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Monitor) Tick(ctx context.Context) error {
	prog, err := m.src.Progress(ctx)
	if err != nil {
		return err
	}
	sess, err := m.fetchSession(ctx)
	if err != nil {
		return err
	}

	now := m.now()
	l := Lag{Applied: prog.Applied, Head: sess.LatestOffset, Speed: sess.Speed, Late: prog.Late}
	if l.LagOffsets = sess.LatestOffset - prog.Applied; l.LagOffsets < 0 {
		l.LagOffsets = 0
	}
	if !m.prevAt.IsZero() {
		if dt := now.Sub(m.prevAt).Seconds(); dt > 0 {
			l.Rate = float64(prog.Applied-m.prevApplied) / dt
		}
	}
	if l.LagOffsets > 0 && l.Rate > 0 {
		l.ETA = float64(l.LagOffsets) / l.Rate
	}
	m.prevApplied, m.prevAt = prog.Applied, now
	m.obs.LagUpdated(l)
	return nil
}

func (m *Monitor) fetchSession(ctx context.Context) (session, error) {
	var s session
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.URL+"/v1/session", nil)
	if err != nil {
		return s, err
	}
	req.Header.Set("Authorization", "Bearer "+m.cfg.Token)
	resp, err := m.client.Do(req)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s, fmt.Errorf("session: status %d", resp.StatusCode)
	}
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

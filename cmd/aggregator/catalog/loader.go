package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const progressEvery = 1000

type Result struct {
	Rows      int64
	Created   int64
	Unchanged int64
	Rejected  int64
	Failed    int64
	Duration  time.Duration
}

type Loader struct {
	Hazards repository.HazardWriter
	Workers int
	Log     *slog.Logger
	Now     func() time.Time
}

func (l Loader) Run(ctx context.Context, r *Reader) (Result, error) {
	start := time.Now()
	var res Result
	rows := make(chan Row, l.Workers*2)

	var wg sync.WaitGroup
	for range l.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range rows {
				l.save(ctx, row, &res)
			}
		}()
	}

	readErr := l.feed(ctx, r, rows, &res)
	close(rows)
	wg.Wait()

	res.Duration = time.Since(start)
	return res, readErr
}

func (l Loader) feed(ctx context.Context, r *Reader, rows chan<- Row, res *Result) error {
	for {
		row, err := r.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case rows <- row:
		case <-ctx.Done():
			return ctx.Err()
		}
		if n := atomic.AddInt64(&res.Rows, 1); n%progressEvery == 0 {
			l.Log.Info("catalog_progress", "rows", n,
				"created", atomic.LoadInt64(&res.Created),
				"unchanged", atomic.LoadInt64(&res.Unchanged),
				"rejected", atomic.LoadInt64(&res.Rejected))
		}
	}
}

func (l Loader) save(ctx context.Context, row Row, res *Result) {
	h, err := ToHazard(row, l.Now())
	if err != nil {
		atomic.AddInt64(&res.Rejected, 1)
		l.Log.Warn("catalog_row_rejected", "line", row.Line, "reason", err.Error())
		return
	}
	outcome, err := l.Hazards.SaveHazard(ctx, h)
	if err != nil {
		atomic.AddInt64(&res.Failed, 1)
		l.Log.Error("catalog_row_failed", "line", row.Line, "hazard_id", h.HazardID, "error", err.Error())
		return
	}
	if outcome == repository.OutcomeCreated {
		atomic.AddInt64(&res.Created, 1)
	} else {
		atomic.AddInt64(&res.Unchanged, 1)
	}
}

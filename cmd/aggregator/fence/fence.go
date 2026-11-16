package fence

import (
	"context"
	"errors"
)

var ErrStaleEpoch = errors.New("stale leader epoch")

type ctxKey struct{}

func WithEpoch(ctx context.Context, epoch int64) context.Context {
	return context.WithValue(ctx, ctxKey{}, epoch)
}

func Epoch(ctx context.Context) (int64, bool) {
	e, ok := ctx.Value(ctxKey{}).(int64)
	return e, ok
}

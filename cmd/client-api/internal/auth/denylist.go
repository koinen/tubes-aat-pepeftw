package auth

import (
	"sync"
	"time"
)

const DefaultDenylistTTL = 5 * time.Minute

type Denylist struct {
	entries sync.Map // jti (string) -> addedAt (time.Time)
	ttl     time.Duration
}

func NewDenylist() *Denylist {
	d := &Denylist{ttl: DefaultDenylistTTL}
	go d.cleanup()
	return d
}

func (d *Denylist) Add(jti string) {
	if jti == "" {
		return
	}
	d.entries.Store(jti, time.Now())
}

func (d *Denylist) IsDenied(jti string) bool {
	if jti == "" {
		return false
	}
	_, ok := d.entries.Load(jti)
	return ok
}

func (d *Denylist) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		d.entries.Range(func(key, value any) bool {
			if addedAt, ok := value.(time.Time); ok {
				if now.Sub(addedAt) > d.ttl {
					d.entries.Delete(key)
				}
			}
			return true
		})
	}
}

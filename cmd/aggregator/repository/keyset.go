package repository

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

var ErrInvalidCursor = errors.New("cursor is invalid")

type Keyset struct {
	At time.Time `json:"t"`
	ID string    `json:"id"`
}

func (k Keyset) Encode() string {
	raw, _ := json.Marshal(k)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func DecodeKeyset(s string) (*Keyset, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var k Keyset
	if err := json.Unmarshal(raw, &k); err != nil || k.ID == "" || k.At.IsZero() {
		return nil, ErrInvalidCursor
	}
	return &k, nil
}

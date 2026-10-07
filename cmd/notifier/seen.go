package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type seenStore struct {
	mu   sync.Mutex
	ids  map[string]struct{}
	file *os.File
}

func openSeenStore(path string) (*seenStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create dedup dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open dedup file: %w", err)
	}

	ids := map[string]struct{}{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if id := strings.TrimSpace(scanner.Text()); id != "" {
			ids[id] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		f.Close()
		return nil, fmt.Errorf("read dedup file: %w", err)
	}
	return &seenStore{ids: ids, file: f}, nil
}

func (s *seenStore) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

func (s *seenStore) Add(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return nil
	}
	if _, err := s.file.WriteString(id + "\n"); err != nil {
		return fmt.Errorf("append dedup file: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync dedup file: %w", err)
	}
	s.ids[id] = struct{}{}
	return nil
}

func (s *seenStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

func (s *seenStore) Close() error {
	return s.file.Close()
}

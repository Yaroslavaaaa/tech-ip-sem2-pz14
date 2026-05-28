package store

import (
	"sync"
)

type ProcessedStore struct {
	mu        sync.RWMutex
	processed map[string]bool
}

func NewProcessedStore() *ProcessedStore {
	return &ProcessedStore{
		processed: make(map[string]bool),
	}
}

// IsProcessed проверяет, обрабатывалось ли сообщение с таким ID
func (s *ProcessedStore) IsProcessed(messageID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processed[messageID]
}

// MarkProcessed отмечает сообщение как обработанное
func (s *ProcessedStore) MarkProcessed(messageID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[messageID] = true
}

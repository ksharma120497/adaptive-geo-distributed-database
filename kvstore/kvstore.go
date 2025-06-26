package kvstore

import "sync"

type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New creates the store.
func New() *KVStore {
	return &KVStore{
		data: make(map[string][]byte),
	}
}

// Put writes or overwrites a key.
func (s *KVStore) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a key, or nil if missing.
func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

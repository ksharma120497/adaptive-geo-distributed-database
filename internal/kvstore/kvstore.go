// internal/kvstore/kvstore.go
package kvstore

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// KVStore holds the in-memory map and a write-ahead log on disk.
type KVStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	wal    *os.File
	writer *bufio.Writer
}

// NewWALStore opens/creates the WAL file and returns a store.
func NewWALStore(path string) (*KVStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &KVStore{
		data:   make(map[string][]byte),
		wal:    f,
		writer: bufio.NewWriter(f),
	}, nil
}

// Replay reads the WAL and replays all entries into the in-memory map.
func (s *KVStore) Replay() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Seek to start and use a reader
	if _, err := s.wal.Seek(0, 0); err != nil {
		return err
	}
	reader := bufio.NewReader(s.wal)
	for {
		// Each record: keyLen(uint32) | key bytes | valLen(uint32) | val bytes
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			break // EOF
		}
		key := make([]byte, keyLen)
		if _, err := reader.Read(key); err != nil {
			return err
		}
		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return err
		}
		val := make([]byte, valLen)
		if _, err := reader.Read(val); err != nil {
			return err
		}
		s.data[string(key)] = val
	}
	return nil
}

// Append writes a key/value pair to the WAL.
func (s *KVStore) Append(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write lengths and bytes
	if err := binary.Write(s.writer, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := s.writer.WriteString(key); err != nil {
		return err
	}
	if err := binary.Write(s.writer, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := s.writer.Write(value); err != nil {
		return err
	}
	return s.writer.Flush()
}

// Set updates the in-memory map after WAL append.
func (s *KVStore) Set(key string, value []byte) {
	s.data[key] = value
}

// Get retrieves a value from the in-memory map.
func (s *KVStore) Get(key string) ([]byte, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Close should be called when shutting down to close the WAL file.
func (s *KVStore) Close() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.wal.Close()
}

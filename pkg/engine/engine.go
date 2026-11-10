package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cicekhasann/walkv/pkg/wal"
)

var (
	ErrKeyNotFound = errors.New("walkv: key not found")
	ErrEmptyKey    = errors.New("walkv: key cannot be empty")
)

type DB struct {
	mu       sync.RWMutex
	memtable map[string][]byte
	wal      *wal.WAL
	dir      string
}

// Open initializes the KV store, recovers state from the WAL file, and opens for writes.
func Open(dir string, policy wal.SyncPolicy) (*DB, int64, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, 0, fmt.Errorf("walkv: create data dir: %w", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	mem := make(map[string][]byte)

	// Replay WAL on startup
	recovered, err := wal.Recover(walPath, func(rec *wal.Record) error {
		k := string(rec.Key)
		switch rec.Op {
		case wal.OpPut:
			v := make([]byte, len(rec.Value))
			copy(v, rec.Value)
			mem[k] = v
		case wal.OpDelete:
			delete(mem, k)
		}
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("walkv: replay failed: %w", err)
	}

	w, err := wal.Open(walPath, policy)
	if err != nil {
		return nil, recovered, fmt.Errorf("walkv: open active wal: %w", err)
	}

	return &DB{
		memtable: mem,
		wal:      w,
		dir:      dir,
	}, recovered, nil
}

// Put writes key and value to WAL and updates the in-memory memtable.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if err := db.wal.Write(wal.OpPut, key, value); err != nil {
		return err
	}

	db.mu.Lock()
	v := make([]byte, len(value))
	copy(v, value)
	db.memtable[string(key)] = v
	db.mu.Unlock()

	return nil
}

// Get retrieves the value associated with key from the in-memory memtable.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	val, ok := db.memtable[string(key)]
	db.mu.RUnlock()

	if !ok {
		return nil, ErrKeyNotFound
	}

	res := make([]byte, len(val))
	copy(res, val)
	return res, nil
}

// Delete writes a tombstone record to WAL and removes the key from the memtable.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if err := db.wal.Write(wal.OpDelete, key, nil); err != nil {
		return err
	}

	db.mu.Lock()
	delete(db.memtable, string(key))
	db.mu.Unlock()

	return nil
}

// Sync forces WAL to flush buffers and fsync to underlying storage.
func (db *DB) Sync() error {
	return db.wal.Sync()
}

// Close flushes active writes and releases file descriptors.
func (db *DB) Close() error {
	return db.wal.Close()
}

// Size returns the count of keys currently in memory.
func (db *DB) Size() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.memtable)
}

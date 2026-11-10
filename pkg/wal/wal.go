package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type SyncPolicy int

const (
	SyncNone   SyncPolicy = 0 // rely on OS page cache
	SyncAlways SyncPolicy = 1 // call fsync on every write
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	policy SyncPolicy
	offset int64
}

// Open creates or appends to a WAL file at the specified path.
func Open(path string, policy SyncPolicy) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("walkv: open wal file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("walkv: stat wal file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriterSize(file, 64*1024), // 64KB write buffer
		policy: policy,
		offset: info.Size(),
	}, nil
}

// Write appends an operation record to the log.
func (w *WAL) Write(op OpType, key, value []byte) error {
	encoded, err := EncodeRecord(op, key, value)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.writer.Write(encoded)
	if err != nil {
		return fmt.Errorf("walkv: write buffer: %w", err)
	}
	w.offset += int64(n)

	if w.policy == SyncAlways {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("walkv: flush buffer: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("walkv: fsync file: %w", err)
		}
	}

	return nil
}

// Sync flushes internal buffers and triggers an OS fsync.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("walkv: flush buffer: %w", err)
	}
	return w.file.Sync()
}

// Close flushes buffers and closes the underlying file descriptor.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("walkv: close flush: %w", err)
	}
	return w.file.Close()
}

// Recover replays an existing WAL file, executing fn for each valid Record.
// If a corrupted or truncated record is found at the tail (e.g. from a system crash),
// it terminates replay gracefully and returns the count of recovered records.
func Recover(path string, fn func(rec *Record) error) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("walkv: recover open: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64*1024)
	var count int64

	for {
		rec, err := DecodeRecord(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == ErrCorruptRecord {
				// Tail crash detected: stop replay and return count
				break
			}
			return count, fmt.Errorf("walkv: decode record at index %d: %w", count, err)
		}

		if err := fn(rec); err != nil {
			return count, fmt.Errorf("walkv: apply record: %w", err)
		}
		count++
	}

	return count, nil
}

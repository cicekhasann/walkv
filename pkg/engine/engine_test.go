package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cicekhasann/walkv/pkg/wal"
)

func TestEngine_BasicOperations(t *testing.T) {
	dir := t.TempDir()

	db, _, err := Open(dir, wal.SyncNone)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("alpha"), []byte("value1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	val, err := db.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Fatalf("expected value1, got %s", string(val))
	}

	if err := db.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = db.Get([]byte("alpha"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestEngine_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write 100 keys and close
	db, _, err := Open(dir, wal.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("sensor:%04d", i)
		v := fmt.Sprintf("metric_data_%d", i*10)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Phase 2: Simulate power outage / crash corruption
	// Corrupt the WAL by appending 7 incomplete bytes (less than a full header)
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatalf("failed to open wal file for corruption test: %v", err)
	}
	_, _ = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x00, 0x05})
	_ = f.Close()

	// Phase 3: Reopen engine after crash
	recoveredDB, count, err := Open(dir, wal.SyncAlways)
	if err != nil {
		t.Fatalf("failed to reopen db after crash: %v", err)
	}
	defer recoveredDB.Close()

	if count != 100 {
		t.Fatalf("expected 100 recovered records, got %d", count)
	}

	// Verify all 100 keys exist intact
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("sensor:%04d", i)
		expectedV := fmt.Sprintf("metric_data_%d", i*10)
		val, err := recoveredDB.Get([]byte(k))
		if err != nil {
			t.Fatalf("key %s missing after crash recovery: %v", k, err)
		}
		if string(val) != expectedV {
			t.Fatalf("value mismatch for %s: got %s, want %s", k, string(val), expectedV)
		}
	}
}

func TestEngine_ConcurrentReadWrite_RaceDetector(t *testing.T) {
	dir := t.TempDir()

	db, _, err := Open(dir, wal.SyncNone)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	const workers = 16
	const opsPerWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers * 2)

	// Writers
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				k := fmt.Sprintf("w:%d:key:%d", workerID, j)
				v := fmt.Sprintf("val:%d", j)
				_ = db.Put([]byte(k), []byte(v))
			}
		}(i)
	}

	// Readers
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				k := fmt.Sprintf("w:%d:key:%d", workerID, j)
				_, _ = db.Get([]byte(k))
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkWAL_Put_Async(b *testing.B) {
	dir := b.TempDir()
	db, _, err := Open(dir, wal.SyncNone)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	key := []byte("bench:key:telemetry")
	val := []byte("packet_sample_payload_data_bytes_128_length_benchmark_stream")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = db.Put(key, val)
	}
}

func BenchmarkMemTable_Get(b *testing.B) {
	dir := b.TempDir()
	db, _, err := Open(dir, wal.SyncNone)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	key := []byte("cached:query:key")
	_ = db.Put(key, []byte("response_payload"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = db.Get(key)
	}
}

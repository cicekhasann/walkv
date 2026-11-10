# walkv

Append-only Write-Ahead Log (WAL) and in-memory key-value storage engine in Go.

Implements crash recovery, binary CRC32 checksumming, and POSIX append semantics for durable persistence.

```
+-------------------------------------------------------------------------+
|                           WAL Record Framing                            |
+-------------------+-----------------+-----------------+-----------------+
|  CRC32 (4 Bytes)  | OpType (1 Byte) | KeyLen (2 Bytes)| ValLen (4 Bytes)|
+-------------------+-----------------+-----------------+-----------------+
|                         Key Bytes [0..KeyLen]                           |
+-------------------------------------------------------------------------+
|                        Value Bytes [0..ValLen]                          |
+-------------------------------------------------------------------------+
```

## Architecture

1. **Write Path:** Every write (`Put` or `Delete`) encodes a binary record with a 32-bit CRC (Castagnoli polynomial) and appends it to disk using `O_APPEND`.
2. **Crash Resilience:** During startup, `walkv` scans the WAL file, verifies frame CRC checksums, and replays valid mutations into an in-memory memtable. If a torn write / power cut occurred, partial bytes at the log tail are detected and cleanly truncated.
3. **Tombstone Semantics:** Deletions are logged as explicit tombstone records (`OpDelete = 0x02`), ensuring deletion state persists across crash recoveries without requiring random writes.

## Benchmarks

Benchmarked on AMD Ryzen 9 6900HX (Linux 6.x):

```
goos: linux
goarch: amd64
pkg: github.com/cicekhasann/walkv/pkg/engine
cpu: AMD Ryzen 9 6900HX with Radeon Graphics
BenchmarkWAL_Put_Async-16      7278229       156.3 ns/op     184 B/op     3 allocs/op
BenchmarkMemTable_Get-16      43559262        24.55 ns/op     16 B/op     1 allocs/op
```

## CLI Usage

```bash
# Build binary
make build

# Store and retrieve keys
./bin/walkv -dir ./data put device:001 '{"temp": 24.2, "status": "nominal"}'
./bin/walkv -dir ./data get device:001
./bin/walkv -dir ./data del device:001

# Run automated crash recovery simulation
./bin/walkv -dir ./data -crash-sim

# Benchmark 100,000 writes
./bin/walkv -dir ./data -bench 100000
```

## Testing

```bash
make test    # Run unit tests & crash recovery verification
make race    # Run thread-safety race detector
make bench   # Run benchmarks
```

## License

MIT License. See [LICENSE](LICENSE).

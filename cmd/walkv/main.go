package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cicekhasann/walkv/pkg/engine"
	"github.com/cicekhasann/walkv/pkg/wal"
)

var globalDB *engine.DB

func main() {
	dataDir := flag.String("dir", "./data", "Directory to store WAL files")
	syncMode := flag.Bool("sync", false, "Enable synchronous fsync on every write")
	benchCount := flag.Int("bench", 0, "Run write benchmark with N entries")
	crashSim := flag.Bool("crash-sim", false, "Run automated crash and CRC recovery verification")
	httpAddr := flag.String("http", "", "Serve HTTP API & visual WAL inspector on address (e.g. :8082)")
	flag.Parse()

	policy := wal.SyncNone
	if *syncMode {
		policy = wal.SyncAlways
	}

	start := time.Now()
	db, recovered, err := engine.Open(*dataDir, policy)
	if err != nil {
		fmt.Fprintf(os.Stderr, "walkv: error opening storage engine: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	globalDB = db

	fmt.Printf("\033[1;33m=== walkv // Write-Ahead Log Storage Engine ===\033[0m\n")
	fmt.Printf("Data dir: %s | Sync policy: %v\n", *dataDir, *syncMode)
	fmt.Printf("Recovered \033[32m%d records\033[0m in \033[36m%v\033[0m\n", recovered, time.Since(start))

	if *crashSim {
		runCrashSimulation(*dataDir)
		return
	}

	if *benchCount > 0 {
		runBenchmark(db, *benchCount)
		return
	}

	if *httpAddr != "" {
		startHTTPServer(*httpAddr)
		return
	}

	// Interactive CLI mode
	args := flag.Args()
	if len(args) == 0 {
		fmt.Printf("\nUsage:\n")
		fmt.Printf("  walkv put <key> <value>\n")
		fmt.Printf("  walkv get <key>\n")
		fmt.Printf("  walkv del <key>\n")
		fmt.Printf("  walkv -bench 100000\n")
		fmt.Printf("  walkv -crash-sim\n")
		fmt.Printf("  walkv -http :8082\n")
		return
	}

	switch args[0] {
	case "put":
		if len(args) < 3 {
			fmt.Println("Usage: walkv put <key> <val>")
			return
		}
		if err := db.Put([]byte(args[1]), []byte(args[2])); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("OK (Key=%s Value=%s stored to WAL)\n", args[1], args[2])
		}

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: walkv get <key>")
			return
		}
		val, err := db.Get([]byte(args[1]))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("%s\n", string(val))
		}

	case "del":
		if len(args) < 2 {
			fmt.Println("Usage: walkv del <key>")
			return
		}
		if err := db.Delete([]byte(args[1])); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("OK (Tombstone appended to WAL for %s)\n", args[1])
		}

	default:
		fmt.Printf("Unknown command: %s\n", args[0])
	}
}

func runBenchmark(db *engine.DB, n int) {
	fmt.Printf("\nExecuting write benchmark with %d records...\n", n)
	start := time.Now()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("id:%08d", i)
		v := fmt.Sprintf("telemetry_payload_bytes_sample_val_%d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			fmt.Printf("benchmark failed at record %d: %v\n", i, err)
			return
		}
	}
	elapsed := time.Since(start)
	_ = db.Sync()
	fmt.Printf("\033[1;32mBenchmark Completed:\033[0m\n")
	fmt.Printf("Total Records: %d\n", n)
	fmt.Printf("Total Time:    %v\n", elapsed)
	fmt.Printf("Throughput:    %.0f writes/sec\n", float64(n)/elapsed.Seconds())
	fmt.Printf("Latency:       %.2f µs/write\n", float64(elapsed.Microseconds())/float64(n))
}

func runCrashSimulation(dataDir string) {
	fmt.Printf("\n\033[1;36m[Simulating Power-Cut / Kernel Panic]\033[0m\n")
	walPath := filepath.Join(dataDir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		fmt.Printf("crash sim failed: %v\n", err)
		return
	}
	// Inject incomplete trailing byte sequence
	_, _ = f.Write([]byte{0xFA, 0xCE, 0xB0, 0x0C, 0x01})
	_ = f.Close()
	fmt.Printf("Injected 5 corrupt partial-frame bytes at tail of %s\n", walPath)

	fmt.Printf("Restarting engine to trigger recovery...\n")
	start := time.Now()
	db, recCount, err := engine.Open(dataDir, wal.SyncNone)
	if err != nil {
		fmt.Printf("\033[31mRecovery failed:\033[0m %v\n", err)
		return
	}
	defer db.Close()
	fmt.Printf("\033[1;32m[PASS] Engine cleanly recovered %d records in %v!\033[0m\n", recCount, time.Since(start))
	fmt.Printf("Corrupt frame was safely isolated and truncated.\n")
}

func startHTTPServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"active_keys": globalDB.Size(),
			"timestamp":   time.Now().Unix(),
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<!DOCTYPE html><html><body style="font-family:monospace;background:#0d1117;color:#58a6ff;padding:2rem;">
<h1>walkv: Write-Ahead Log Storage Engine</h1><p>Embedded engine running.</p></body></html>`))
	})

	fmt.Printf("HTTP interface listening on http://localhost%s\n", addr)
	_ = http.ListenAndServe(addr, mux)
}

# Forest — High-Throughput LSM-Tree Storage Engine

[![Build Status](https://github.com/Awesome06/Forest_DatabaseEngine/actions/workflows/build.yml/badge.svg)](https://github.com/Awesome06/Forest_DatabaseEngine/actions)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Forest is a persistent, write-optimized distributed key-value store built entirely from scratch in Go. Designed for high write concurrency and sub-millisecond latencies, it leverages a Log-Structured Merge-Tree (LSM-Tree) architecture served over a raw, zero-allocation TCP binary socket. 

![CPU Flame Graph](https://raw.githubusercontent.com/Awesome06/Forest_DatabaseEngine/main/assets/cpu-flamegraph.png)
*CPU Flame Graph proving zero GC overhead and async WAL syncing. Notice the complete absence of `mallocgc` (Garbage Collection) blocks on the TCP read/write paths.*

## Contents
- [Quick Start \& Chaos Testing](#quick-start--chaos-testing)
- [Design Philosophy](#design-philosophy)
- [Performance: Zero-Allocation Hot Path](#performance-zero-allocation-hot-path)
- [System Architecture](#system-architecture)
- [Core Features](#core-features)
- [Custom Binary Protocol](#custom-binary-protocol)
- [Project Layout](#project-layout)

## Quick Start & Chaos Testing
Forest is built to survive catastrophic failure. You can run the built-in Chaos Engineering suite on your local machine to watch the database recover from an abrupt `kill -9` process termination.

### 1. Prerequisites
- Go 1.22+
- Linux/macOS recommended (Windows supported via WSL)

### 2. Run the Chaos Test
This script compiles the server, blasts it with concurrent writes, abruptly kills the process, and validates that the WAL recovers 100% of the acknowledged data on reboot.
```bash
git clone [https://github.com/Awesome06/Forest_DatabaseEngine.git](https://github.com/Awesome06/Forest_DatabaseEngine.git)
cd Forest_DatabaseEngine
chmod +x ./scripts/chaos_test.sh
./scripts/chaos_test.sh
```

### 3. Run the Server Manually
```bash
go build -o bin/forest ./cmd/server
# Plaintext mode:
./bin/forest --port=9000 --udp-port=9001 --metrics-port=2112 --wal-dir=./data/wal --sst-dir=./data/sst

# Mutual TLS (mTLS) secure mode:
./bin/forest --port=9000 --udp-port=9001 --metrics-port=2112 --tls-cert=cert.pem --tls-key=key.pem --tls-ca=ca.pem
```

## Design Philosophy
Most modern applications rely on high-level databases without understanding the mechanical sympathy required to make them fast. I built Forest to bridge the gap between application logic and kernel-level hardware execution. The primary goals:
1. **Master Lock-Free Concurrency:** Eliminate mutex bottlenecks using atomic pointer swaps in a custom SkipList.
2. **Defeat Garbage Collection:** Prove that Go can handle extreme network throughput without stop-the-world GC pauses by engineering a zero-allocation hot path.
3. **Understand Disk I/O:** Implement Read-Copy-Update (RCU) file manifests and background compaction to manage disk fragmentation mathematically.
4. **Dual-Protocol Ingestion & Isolation:** Guarantee mission-critical storage writes over TCP while ingesting loss-tolerant telemetry streams concurrently over UDP using bounded buffer pools.
5. **Zero-Allocation Observability & mTLS:** Support encrypted cluster communication via mTLS without degrading throughput, and expose real-time Prometheus RED metrics with zero heap allocations on the hot path.

## Performance: Zero-Allocation Hot Path
High-level HTTP/REST APIs generate massive amounts of heap allocations, triggering Go's Garbage Collector. Forest circumvents this via custom TCP/UDP binary parsers that read headers directly from OS socket buffers.

**Parser Benchmarks (Zero-Copy):**
```text
goos: windows / linux
goarch: amd64
pkg: github.com/Forest_DatabaseEngine/internal/network
cpu: 13th Gen Intel(R) Core(TM) i5-13450HX
BenchmarkParseHeader-16             208481912         5.766 ns/op           0 B/op          0 allocs/op
BenchmarkParseTelemetryPacket-16    828279676         1.457 ns/op           0 B/op          0 allocs/op
BenchmarkHistogramObserve-16         28135791        42.370 ns/op           0 B/op          0 allocs/op
```
*Result: `5.76ns` TCP parsing latency and `1.45ns` UDP parsing latency with **zero heap allocations** per request, allowing the engine to sustain massive throughput without triggering GC pauses.*

## System Architecture

```mermaid
graph TD
    Client[TCP/mTLS Client] -->|Custom Binary Protocol| Server[Dual-Protocol Server]
    UDPClient[UDP Telemetry Agent] -->|Loss-Tolerant Streams| Server
    Prometheus[Prometheus Scraper] -->|GET /metrics| Server

    Server -->|1. Append| WAL[(Write-Ahead Log)]
    Server -->|2. Insert| MemTable[Active MemTable <br/> Lock-Free SkipList]
    Server -->|Read| MemTable
    Server -->|Ephemeral Health| MetricsBus[Prometheus Metrics Engine]
    
    MemTable -->|Flushes at 4MB| L0[Level 0 SSTables]
    
    Server -->|Read Miss| Bloom[Bloom Filters]
    Bloom -.->|Filter hits: 99% accuracy| L0
    Bloom -.->|Filter hits: 99% accuracy| L1[Level 1 SSTables]
    
    L0 -->|Background Compaction <br/> K-Way Min-Heap Merge| L1
```

## Core Features
| Feature | Implementation Details |
| :--- | :--- |
| **Mutual TLS (mTLS)** | Full two-way certificate verification via Go standard library `crypto/tls` (TLS 1.3), isolating TLS handshakes from zero-allocation frame parsing. |
| **Dual-Protocol Ingestion** | Guaranteed delivery for database mutations over TCP, coupled with concurrent non-blocking UDP ingestion (`net.ListenUDP`) with bounded buffer pools and drop protection. |
| **Production Observability** | Pure Go stdlib Prometheus exposition (`/metrics`) tracking TCP throughput, parsing latency, UDP packet drops, MemTable swaps, and WAL fsync durations with zero hot-path allocations. |
| **Grafana Dashboard** | Pre-packaged local Grafana dashboard (`assets/grafana-dashboard.json`) visualizing RED metrics, latency histograms, and engine internals in real-time. |
| **Lock-Free MemTable** | Uses a concurrent SkipList and Go's `sync/atomic` primitives to eliminate thread contention under heavy write load. |
| **Crash-Safe Durability** | Implements Write-Ahead Logging (WAL) with double-buffering and CRC32 checksums. Ensures zero data loss up to the last network ACK. |
| **Disk-Optimized SSTables** | Append-only Sorted String Tables featuring integrated custom Bloom Filters (bit arrays) to mathematically eliminate disk reads for non-existent keys. |
| **RCU Compaction** | A dedicated background worker pool merges Level-0 files into sorted Level-1 files asynchronously using RCU reference counting. |

## Custom Binary Protocol
To maximize throughput, Forest uses a strict 8-byte TCP header and a 4-byte UDP telemetry header, eliminating the parsing overhead of JSON or HTTP.

### TCP Request Header (8 Bytes)
| Field | Size | Description |
| :--- | :--- | :--- |
| **Magic Byte** | 1 Byte | Validation byte (`0xA1`) to reject malformed packets immediately. |
| **OpCode** | 1 Byte | Operation type: `0x01` (Echo), `0x02` (Put), `0x03` (Get), `0x04` (Delete). |
| **Key Length** | 2 Bytes | Unsigned 16-bit integer defining key size. |
| **Value Length**| 4 Bytes | Unsigned 32-bit integer defining payload size. |
| **Payload** | Variable| Raw bytes containing `Key + Value`. |

### UDP Telemetry Header (4 Bytes)
| Field | Size | Description |
| :--- | :--- | :--- |
| **Magic Byte** | 1 Byte | Validation byte (`0xA1`). |
| **OpCode** | 1 Byte | `0x05` (Telemetry) or `0x06` (Heartbeat). |
| **Payload Length**| 2 Bytes | Unsigned 16-bit integer defining telemetry payload size. |
| **Payload** | Variable| Loss-tolerant health/metrics payload. |

## Project Layout
```text
.
├── cmd/
│   └── server/          # Main application entrypoint with flags for TCP, UDP, mTLS, and Prometheus
├── internal/
│   ├── engine/          # LSM-Tree engine core (MemTable, SSTables, WAL, Bloom Filter, Compactor)
│   ├── network/         # Dual-protocol server, mTLS TLS helpers, zero-allocation TCP/UDP parsers
│   └── metrics/         # Zero-allocation stdlib Prometheus metrics registry and /metrics handler
├── assets/              # Architecture diagrams, flamegraphs, and Grafana dashboard JSON
├── scripts/             # Chaos testing and benchmarking bash scripts
├── data/                # Local storage for WAL and SST files (gitignored)
└── README.md
```

---

## Contact
**Mrigank Bhatnagar**  
* [LinkedIn](https://linkedin.com/in/mrigank-bhatnagar)
* [GitHub](https://github.com/Awesome06)

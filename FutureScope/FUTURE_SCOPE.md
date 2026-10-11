# Future Scope & Technical Roadmap: ForestDB

> **Living Document Governance — Forward-Facing Always**  
> This roadmap is strictly, exclusively forward-facing. It catalogues prospective, unbuilt technical milestones and architectural scaling goals for ForestDB.  
> **The Lifecycle Rule:** The moment a feature or milestone is implemented, tested, and verified, it is permanently excised from this document and transitioned directly into [README.md](file:///c:/Users/Bhatn/OneDrive/Desktop/Projects/github/Forest_DatabaseEngine/README.md) and core system documentation. No historical or completed tags (`[COMPLETED]`) are retained here, ensuring this document always serves as an active, uncompromised forward vision.

---

### Architectural Context
ForestDB currently provides a production-grade single-node storage engine featuring:
- A lock-free SkipList `MemTable` engineered with `sync/atomic` primitives.
- Append-only Write-Ahead Logging (WAL) with CRC32 integrity validation.
- Level-0 to Level-1 SSTables with custom Bloom Filters and background K-Way merge compaction under Read-Copy-Update (RCU) safety.
- Dual-protocol concurrent listeners (guaranteed TCP storage mutations + loss-tolerant UDP telemetry).
- Zero-allocation binary framing and an embedded stdlib Prometheus RED metrics exporter.
- TLS 1.3 Mutual Authentication (mTLS) with near zero throughput overhead.
- Multi-tier ordered range queries (`Scan(startKey, endKey)`) and the official Go Client SDK (`pkg/client`).

The milestones below define the trajectory for scaling ForestDB into a distributed, multi-tenant, hardware-saturating database cluster.

---

### Phase 1: Near-Term Storage Kernel Optimizations
*   **Dynamic Variable-Length Memory Pooling:** Expand `sync.Pool` recycling to handle variable key and value sizes without falling back to OS heap allocations on oversized mutation batches.
*   **Exact Struct & Pointer Byte Accounting:** Refine `MemTable` capacity calculations to account for internal 64-bit struct padding, pointer overhead, and SkipList node height variations, guaranteeing rigid memory ceilings before triggering a flush.
*   **Vectorized Binary Unpacking:** Evaluate SIMD/vectorized assembly instructions for multi-record batch deserialization over high-bandwidth loopback interfaces.

---

### Phase 2: Distributed Consensus & Replication (Raft Engine)
To evolve ForestDB from a single-node engine into a fault-tolerant distributed cluster:
*   **gRPC WAL Stream Replication:** Instead of writing solely to local disk, the leader node will asynchronously stream WAL entries to follower nodes over low-latency gRPC streams.
*   **Automated Leader Election & Heartbeat Monitors:** Implement Raft randomized election timeouts (150ms–300ms) to ensure seamless cluster failover if the active leader node terminates.
*   **Zero-Downtime Cluster Reconfiguration:** Support dynamic cluster membership changes, allowing nodes to join or leave without pausing client write pipelines.
*   **Consistent Hash Partitioning:** Introduce an orchestration and sharding layer to partition key ranges across distinct multi-Raft consensus groups, achieving horizontal write scaling across multiple machines.

---

### Phase 3: Linux Kernel Bypass & Hardware Saturation
Currently, ForestDB relies on standard POSIX system calls (`os.Write`, `os.ReadAt`) and the operating system page cache. We plan to bypass these abstraction layers for maximum I/O saturation:
*   **`io_uring` Asynchronous Disk Engine:** Replace blocking POSIX system calls on Linux with `io_uring` submission/completion queues to achieve true asynchronous, zero-copy disk I/O, drastically reducing context switches during heavy background compactions.
*   **Memory-Mapped File Operations (`mmap`):** Utilize `mmap` for SSTable read paths, allowing the kernel to page in SSTable blocks transparently and eliminating userspace `ReadAt` buffer copies.
*   **Direct I/O (`O_DIRECT`) & In-Memory LRU Block Cache:** Bypass the kernel page cache entirely for write paths to eliminate page-cache pollution. Implement an in-memory Least Recently Used (LRU) Block Cache directly inside the Go runtime to maintain strict memory isolation during massive sequential scans.
*   **Block-Level Compression (LZ4 / Zstandard):** Integrate streaming LZ4 and Zstandard algorithms for SSTable data blocks, trading minimal CPU overhead for up to 60% disk footprint reduction and reduced physical I/O bandwidth.

---

### Phase 4: LSM Query Maturity & Transactional Semantics
*   **Strict Multi-Tier Leveled Compaction ($L_0 \rightarrow L_n$):** Expand the background compactor to cascade merges down to $L_2, L_3, \dots, L_n$, enforcing rigid 10x size amplification boundaries per level to mathematically bound read amplification.
*   **Multi-Version Concurrency Control (MVCC) & Snapshot Isolation:** Assign monotonic transaction sequence IDs to writes, allowing long-running read transactions to view stable historical snapshots without blocking concurrent incoming writes.
*   **Time-To-Live (TTL) Key Pruning:** Append optional microsecond timestamps to keys and discard expired entries lazily during background SSTable compaction merges.
*   **Secondary Indexes:** Implement asynchronous, non-blocking secondary indexes enabling range queries over non-primary fields.

---

### Phase 5: Enterprise Multi-Tenancy & Access Governance
*   **Role-Based Access Control (RBAC):** Introduce a token-based authentication and permissions system directly inside the binary protocol framing to support granular read/write/admin capabilities.
*   **Cryptographic Tenant Isolation:** Implement virtual namespaces partitioning keyspaces with dedicated cryptographic encryption keys per tenant.

---

### Phase 6: Developer Ecosystem & Tooling
*   **`forest-cli` Offline Binary Inspector:** Build a standalone command-line introspection tool to parse, inspect, and repair `.sst` data blocks, Bloom Filter bit arrays, and WAL segments directly on disk for debugging.
*   **Polyglot Client SDKs:** Develop idiomatic, high-throughput client SDKs in **Rust** and **Python** mirroring the official Go Client SDK (`pkg/client`), featuring built-in connection pooling, mTLS, and automatic shard routing.
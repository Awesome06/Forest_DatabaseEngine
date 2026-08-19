# Future Scope & Roadmap: ForestDB

While the current version of ForestDB successfully implements a single-node, high-throughput, lock-free LSM-Tree storage engine, the architecture was explicitly designed to act as the foundational storage layer for a broader distributed system. 

The following milestones outline the technical roadmap for scaling ForestDB into a highly available, enterprise-grade database cluster. We have also introduced a **Phase 1: Near-Term Engine Optimizations** to directly address immediate architectural improvements before scaling out.

### Phase 1: Near-Term Engine Optimizations
Before moving to distributed consensus, we will address the immediate architectural improvements to solidify the single-node engine:
*   **Dynamic Memory Pooling:** Implement `sync.Pool` for payload buffers to support dynamic key/value sizes while maintaining zero-allocation properties, reducing memory footprint for idle connections.
*   **Accurate Memory Tracking:** Refine `MemTable` size calculations to accurately track struct and pointer overhead to prevent edge-case OOMs.
*   **I/O Latency Monitoring & Backpressure:** Introduce metrics around disk flush latencies to ensure slow I/O does not unknowingly block the network ingestion layer.
*   **Heuristic-Based Compaction:** Upgrade from fixed `CompactL0toL1` to a dynamic trigger based on L0 file counts and level size multipliers.

### Phase 2: Distributed Consensus & Replication
To move from a single-node engine to a highly available cluster, we will implement the **Raft Consensus Algorithm**.
*   **WAL Replication:** Instead of only syncing the Write-Ahead Log to the local disk, the leader node will replicate WAL entries to follower nodes over **gRPC**.
*   **Leader Election & Failover:** Automatic failover handling. If the leader node crashes (simulated in our Chaos tests), the cluster will seamlessly elect a new leader using Raft randomized timeouts.
*   **Dynamic Cluster Membership:** Support seamlessly adding or removing nodes from the cluster without downtime.
*   **Consistent Hashing:** Implement a sharding layer to distribute key ranges across multiple distinct Raft clusters, enabling horizontal write scaling.

### Phase 3: Advanced I/O, Memory, & Storage Optimization
Currently, ForestDB relies on standard POSIX system calls (`os.Write`, `os.ReadAt`) and the OS Page Cache. We plan to bypass these for maximum hardware utilization.
*   **`io_uring` Integration:** Swap the standard blocking I/O layer on Linux with `io_uring` to achieve true zero-copy, asynchronous disk I/O, drastically reducing context switches during SSTable compactions.
*   **Memory-Mapped Files (`mmap`):** Utilize memory-mapped I/O for SSTable reads, allowing the OS to page in data blocks transparently and eliminating explicit `ReadAt` buffer copies.
*   **Direct I/O (`O_DIRECT`) & Custom Cache:** Bypass the Linux kernel page cache entirely for writes. We will implement a custom LRU (Least Recently Used) Block Cache inside the Go runtime to maintain strict control over memory usage and prevent page-cache thrashing during large read scans.
*   **Block-Level Compression:** Integrate **Zstandard (zstd)** or **LZ4** compression algorithms for SSTable data blocks, significantly trading a small amount of CPU overhead for drastically reduced disk space and I/O bandwidth.

### Phase 4: LSM-Tree Maturity & Query Capabilities
The current engine supports L0 to L1 background compaction and exact-match point queries (`Get`, `Put`, `Delete`). 
*   **Strict Leveled Compaction:** Expand the K-Way merge compactor to support cascading compaction down to $L_n$ (e.g., L1 $\rightarrow$ L2 $\rightarrow$ L3), strictly enforcing size amplifiers (e.g., each level is 10x larger than the previous) to bound read amplification mathematically.
*   **MVCC & Snapshot Isolation:** Implement Multi-Version Concurrency Control using the existing `FileSequenceID`. This will allow long-running read transactions to view a stable snapshot of the database without blocking incoming writes.
*   **Range Queries:** Implement `Scan(startKey, endKey)` by logically merging the SkipList iterator with the SSTable streaming iterators to serve ordered range scans efficiently.
*   **Time-To-Live (TTL):** Support automatic key expiration by appending a timestamp to the MemTable entry and dropping expired keys lazily during background compaction.
*   **Secondary Indexing:** Introduce asynchronous secondary indexes to support querying data beyond just primary key lookups.

### Phase 5: Enterprise Security & Observability
To ensure ForestDB is ready for production workloads in heavily regulated environments:
*   **TLS/mTLS Encryption:** Secure the raw TCP binary protocol with Transport Layer Security, ensuring data-in-transit is encrypted between clients and across Raft cluster nodes.
*   **Prometheus Metrics & Profiling:** Expose an HTTP `/metrics` endpoint to monitor cache hit rates, WAL flush latencies, compaction write amplification, and active connection counts in real time.
*   **Role-Based Access Control (RBAC):** Implement lightweight, token-based authentication and authorization for the binary protocol.

### Phase 6: Ecosystem & Tooling
*   **CLI Introspection Tool:** Build a standalone CLI utility (e.g., `forest-cli inspect`) that can parse and print the binary structures of `.sst` files, Bloom Filters, and WALs offline for debugging.
*   **Official Client SDKs:** Develop idiomatic client libraries in Go, Rust, and Python that natively speak the zero-allocation Forest binary protocol, complete with connection pooling and automatic leader-discovery routing.
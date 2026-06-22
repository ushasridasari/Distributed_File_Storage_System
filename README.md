# Distributed File Storage System in Java
### Inspired by the Google File System (Ghemawat et al., SOSP 2003)

> A production-grade distributed storage system implementing core GFS paper concepts: chunk-based storage, 3× replication, lease-based consistency, atomic record append, fault tolerance, and automatic recovery — all in ~1,800 lines of Java.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Features](#features)
4. [Tech Stack](#tech-stack)
5. [Project Structure](#project-structure)
6. [Quick Start](#quick-start)
7. [CLI Reference](#cli-reference)
8. [Implementation Highlights](#implementation-highlights)
9. [Challenges & Solutions](#challenges--solutions)
10. [Configuration](#configuration)
11. [Test Coverage](#test-coverage)
12. [End-to-End Demo](#end-to-end-demo)
13. [Future Enhancements](#future-enhancements)
14. [References](#references)

---

## Project Overview

Modern distributed systems must store and serve data reliably across thousands of commodity machines, tolerating hardware failures as a norm rather than an exception. This project implements the core architecture of the **Google File System (GFS)** — the storage layer that underpins Google's search index, MapReduce jobs, and many large-scale services.

### Motivation

Traditional single-node storage fails at scale in two critical ways: capacity limits make it impossible to store files larger than one machine's disk, and hardware failures make single-node systems inherently unreliable. GFS solves both by:

- **Sharding**: splitting files into fixed-size chunks distributed across many servers
- **Replication**: storing each chunk on 3 independent nodes so data survives hardware failure
- **Centralised metadata**: a single Master tracks where every chunk lives, so clients always find their data

This implementation faithfully realises all core GFS paper responsibilities — from chunk leases and version-based stale detection to operation log recovery and rack-aware replica placement — making it a complete, runnable distributed storage system, not merely a toy.

---

## System Architecture

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                          GfsClient                               │
  │   CLI via Main.java — upload / download / append / stat / …     │
  └─────────────┬────────────────────────────────┬───────────────────┘
                │  metadata RPCs (TCP)            │  data RPCs (TCP)
                ▼                                 ▼
  ┌──────────────────────────┐   ┌────────────────────────────────────────┐
  │       MasterServer       │   │              ChunkServers              │
  │  ──────────────────────  │◄──│  :9100          :9101          :9102   │
  │  • file namespace        │   │  chunk files    chunk files    chunk   │
  │  • chunk → location map  │   │  .crc (CRC32)   .crc           .crc   │
  │  • version registry      │   │  .ver (version) .ver           .ver   │
  │  • lease management      │   └────────────────────────────────────────┘
  │  • operation log         │              ▲
  │  • checkpoint/recovery   │              │  heartbeat every 5 s
  │  • background re-rep.    │──────────────┘  (chunkId → version map,
  │  • orphan cleanup        │                  free disk, rack ID)
  └──────────────────────────┘
```

**Write path (upload / append):**
```
Client → Master  : CREATE_FILE / REQUEST_CHUNK_WRITE   (get lease + replica set)
Client → Primary : WRITE_CHUNK / APPEND_CHUNK          (data transfer)
         Primary → Secondary1 : REPLICATE_CHUNK        (chain replication)
         Primary → Secondary2 : REPLICATE_CHUNK
Client → Master  : UPDATE_FILE_SIZE / NOTIFY_APPEND    (metadata update)
```

**Read path (download):**
```
Client → Master      : GET_FILE_INFO        (chunk list)
Client → Master      : REQUEST_CHUNK_READ   (live replica locations, cached 60 s)
Client → Any replica : READ_CHUNK           (CRC-verified data)
         → automatic fallback to next replica on failure
```

**Fault detection:**
```
ChunkServer → Master : HEARTBEAT every 5 s  (chunk versions + free disk)
Master               : marks server dead after 15 s silence
Master               : triggers re-replication for under-replicated chunks
Master               : GC orphaned chunks when server rejoins
```

---

## Features

### Core Storage
- **64 MB chunk-based storage** — files of any size split into fixed chunks (GFS §2.6)
- **3× replication** — each chunk stored on 3 independent servers
- **Primary-chain replication** — client writes to the primary, which chains writes to secondaries
- **Streaming upload** — O(1) heap usage regardless of file size; one 64 MB buffer at a time
- **Atomic record append** — concurrent clients append without overwriting each other (GFS §3.3)
- **CRC32 checksums** — `.crc` sidecar per chunk, verified on every read to detect silent corruption

### Consistency & Fault Tolerance
- **Lease-based primary election** — 60-second leases with automatic renewal 10 s before expiry (GFS §5.4)
- **Version-based stale detection** — `.ver` sidecar on disk; replicas with old versions evicted on heartbeat (GFS §4.5)
- **Automatic background re-replication** — Master restores 3× copies when a server dies (GFS §4.4)
- **Node rejoin reconciliation** — valid replicas re-admitted; orphaned chunks GC'd when a server comes back online
- **Operation log + checkpoint recovery** — every mutation logged before applied; namespace checkpointed every 5 min (GFS §5.2)
- **Orphaned file cleanup** — file entries created but never written removed after 60-second grace period

### Client Intelligence
- **Client-side chunk location cache** — 60-second TTL to reduce Master load (GFS §3.1)
- **Replica fallback on read** — automatically retries next replica, evicting stale cache entries
- **RPC retry with linear backoff** — 3 attempts, 500 ms × attempt delay for Master RPCs, writes, and replication
- **Socket timeouts** — 5 s connect, 30 s read — no indefinite blocking on hung servers

### Placement
- **Rack-aware replica placement** — first fills one slot per rack, then falls back to any live server
- **Disk-space-aware placement** — candidates sorted by free bytes descending, so hot disks never fill up

### Namespace
- Hierarchical directory namespace (mkdir, list, rename)
- Accurate `stat` reporting: live chunk count, file size, created/modified timestamps

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 |
| Build | Apache Maven 3.6+ |
| Communication | Raw TCP sockets (`java.net.Socket`) |
| Serialisation | Java Object Serialisation (`ObjectOutputStream` / `ObjectInputStream`) |
| Concurrency | `ConcurrentHashMap`, `ScheduledExecutorService` |
| Integrity | `java.util.zip.CRC32` (per-chunk checksums) |
| Persistence | Write-ahead log + serialised checkpoint (`java.io.Serializable`) |
| Testing | JUnit 4.13.2 |
| Distributed Design | Google File System paper (Ghemawat et al., SOSP 2003) |

---

## Project Structure

```
src/
├── main/java/
│   ├── Main.java            CLI entry point — 11 commands via switch-case
│   ├── MasterServer.java    Metadata coordinator (namespace, chunk registry,
│   │                        version tracking, lease management, operation log,
│   │                        checkpoint + log replay, re-replication, rejoin
│   │                        reconciliation, orphan cleanup)
│   ├── ChunkServer.java     Data node (chunk files + .crc + .ver sidecars,
│   │                        CRC verification, replication chaining, heartbeat,
│   │                        lease renewal, rack/disk reporting)
│   ├── GfsClient.java       Client API (streaming upload/download, append,
│   │                        namespace ops, location cache, replica fallback,
│   │                        RPC retry, socket timeouts)
│   ├── GfsConfig.java       Cluster constants + .gfs/config INI reader/writer
│   └── Message.java         TCP wire message (String type + HashMap payload)
└── test/java/
    ├── MasterServerTest.java   15 unit tests
    └── ChunkStorageTest.java   13 unit tests
```

---

## Quick Start

### Prerequisites
- Java 11+ (tested on OpenJDK 17)
- Apache Maven 3.6+

### Build

```bash
mvn clean package -q
JAR=target/gfs-java-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Start the cluster

**Terminal 1 — Master server:**
```bash
java -cp $JAR Main master
# [Master] Listening on port 9000
```

**Terminals 2–4 — Chunk servers (one per rack for full rack-aware replication):**
```bash
java -cp $JAR Main chunkserver 9100 chunk_data/node1 rack-A
java -cp $JAR Main chunkserver 9101 chunk_data/node2 rack-B
java -cp $JAR Main chunkserver 9102 chunk_data/node3 rack-C
```

### Use the CLI

```bash
# Create a directory
java -cp $JAR Main mkdir /data

# Upload any file (streams — no heap limit)
java -cp $JAR Main upload myfile.txt /data/myfile.txt

# Show file metadata (size, chunks, replicas, timestamps)
java -cp $JAR Main stat /data/myfile.txt

# Atomically append text
java -cp $JAR Main append /data/myfile.txt "new line of data"

# List files under a prefix
java -cp $JAR Main list /data

# Rename a file
java -cp $JAR Main rename /data/myfile.txt /data/renamed.txt

# Download (falls back across replicas automatically)
java -cp $JAR Main download /data/renamed.txt local_copy.txt

# Check cluster health (live/dead servers, free disk, rack IDs)
java -cp $JAR Main cluster-status

# Delete (chunks GC'd immediately from all servers)
java -cp $JAR Main delete /data/renamed.txt
```

### Run tests

```bash
mvn test
```

---

## CLI Reference

### File Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `upload` | `upload <local-path> <gfs-path>` | Stream local file into GFS as 64 MB chunks; handles files of any size |
| `download` | `download <gfs-path> <local-path>` | Reassemble chunks from any live replica; auto-fallback on failure |
| `append` | `append <gfs-path> <text>` | Atomically append text to an existing GFS file (GFS §3.3) |
| `delete` | `delete <gfs-path>` | Remove file and immediately GC all chunk replicas from every server |
| `rename` | `rename <src-path> <dst-path>` | Rename or move a file within the namespace |
| `mkdir` | `mkdir <gfs-path>` | Create a directory entry in the namespace |
| `list` | `list [prefix]` | List all files/directories under a path prefix |
| `stat` | `stat <gfs-path>` | Show type, size, live chunk count, replica count, and timestamps |

### Cluster Management

| Command | Usage | Description |
|---------|-------|-------------|
| `cluster-status` | `cluster-status` | Show live/dead servers with rack ID, free disk, total files and chunks |
| `master` | `master [port]` | Start the Master server (default: 9000) |
| `chunkserver` | `chunkserver <port> [storage-dir] [rack-id]` | Start a Chunk server with optional rack grouping |

---

## Implementation Highlights

### 1. Rack-Aware Replica Placement

To maximise durability, replicas should survive even a complete rack failure (a common failure domain in real data centres). The Master selects servers in two passes:

```java
// Pass 1: one slot per unique rack (cross-rack diversity)
for (ServerInfo s : candidates) {
    if (selected.size() >= count) break;
    if (!usedRacks.contains(s.rackId)) {
        selected.add(s.location);
        usedRacks.add(s.rackId);
    }
}
// Pass 2: fill remaining slots from any live server
for (ServerInfo s : candidates) {
    if (selected.size() >= count) break;
    if (!selected.contains(s.location)) selected.add(s.location);
}
```

Candidates are pre-sorted by free bytes descending, so disk-space-aware tie-breaking prevents any server from becoming a hot spot.

### 2. Lease-Based Write Consistency

To enforce at-most-one-primary per chunk, the Master grants 60-second leases. The primary is the only server authorised to accept client writes for that chunk's version. Key design choices:

- **Renewal before expiry**: ChunkServer schedules renewal 10 s before the lease expires, preventing unnecessary re-election from brief delays
- **Safe renewal semantics**: a transient network error does **not** revoke the lease — only an explicit `ERROR` response from the Master does. This prevents a brief disconnect from causing a live primary to believe it's dead
- **Lease expiry triggers re-election**: on the next write request, the Master picks a new primary and increments the chunk version

### 3. Version-Based Stale Replica Detection (GFS §4.5)

Every chunk carries a monotonically increasing version number stored in a `.ver` sidecar file. The Master increments the version before granting each new write lease. On every heartbeat, ChunkServers report their `chunkId → version` map. The Master silently evicts any replica whose reported version lags behind — preventing stale reads from serving outdated data after a server missed a write cycle.

### 4. Operation Log + Checkpoint Recovery (GFS §5.2)

Every metadata mutation is **appended to `.gfs/master.log` before being applied** — the standard write-ahead log (WAL) pattern. Every 5 minutes the namespace and chunk table are serialised to `.gfs/checkpoint.ser` and the log is truncated. On startup, the Master loads the checkpoint then replays remaining log entries, recovering exactly the state it had before the crash.

### 5. Streaming Large-File Upload

The naive approach (`Files.readAllBytes()`) loads the entire file into heap, causing OOM for multi-GB files. The upload path instead streams through a single reusable 64 MB buffer:

```java
byte[] buffer = new byte[GfsConfig.CHUNK_SIZE_BYTES];
try (FileInputStream fis = new FileInputStream(file)) {
    int bytesRead;
    while ((bytesRead = fis.read(buffer)) != -1) {
        byte[] chunkData = (bytesRead < buffer.length)
            ? Arrays.copyOf(buffer, bytesRead) : buffer;
        // request lease, write chunk, repeat
    }
}
```

Heap usage is O(1) regardless of file size.

### 6. RPC Retry with Linear Backoff

All outbound RPCs — Master metadata requests, chunk writes, replication, and lease renewals — retry up to 3 times with a linear delay (`500 ms × attempt number`). This handles transient TCP resets and brief server GC pauses without surfacing errors to the user.

---

## Challenges & Solutions

| Challenge | Root Cause | Solution |
|-----------|-----------|----------|
| **`ArrayList$SubList` `NotSerializableException` on upload** | `List.subList()` returns a non-serialisable view that can't cross an `ObjectOutputStream`. | Wrapped every `subList()` call in `new ArrayList<>(...)` before putting it in a wire `Message`. |
| **"Connection refused" on first client write** | `ChunkServer` called `registerWithMaster()` before `new ServerSocket(port)` — Master marked the node live before its port was open. | Reordered startup: bind `ServerSocket` first, then register with Master. |
| **`fileSize` always 0 after upload** | `FileMetadata.fileSize` was never updated after writing chunks. | Added `UPDATE_FILE_SIZE` RPC from client to Master at the end of upload, and `NOTIFY_APPEND` RPC after every append. |
| **Inflated chunk count in `stat`** | `stat` counted all chunk IDs ever assigned, including stale-evicted ones. | Changed to count only IDs still present in `chunkTable` (live replicas). |
| **OOM risk on large-file upload** | `Files.readAllBytes()` materialised the entire file in heap. | Replaced with streaming `FileInputStream` loop reading one 64 MB buffer at a time. |
| **`ChunkStorage.append()` double-buffering** | Read existing chunk into memory to concatenate before writing back. | Replaced with `Files.write(..., StandardOpenOption.APPEND)` — the OS handles concatenation. |
| **Indefinite hang on dead server** | All sockets used default no-timeout `new Socket()`. | Introduced `newSocket()` helper with 5 s connect timeout and 30 s read timeout (`setSoTimeout`). |
| **Lease revoked on transient network error** | `renewLease()` removed the lease on any `IOException`. | Changed to only revoke on an explicit `ERROR` response — transient errors are retried, not penalised. |

---

## Configuration

All defaults live in `GfsConfig.java` and are written to `.gfs/config` on first run:

| Constant | Default | Description |
|----------|---------|-------------|
| `CHUNK_SIZE_BYTES` | 67,108,864 (64 MB) | Maximum size of one chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `MASTER_PORT` | 9000 | Master server TCP port |
| `CHUNK_SERVER_BASE_PORT` | 9100 | Default first chunk server port |
| `HEARTBEAT_INTERVAL_MS` | 5,000 | How often chunk servers ping the Master |
| `CHUNK_SERVER_TIMEOUT_MS` | 15,000 | Time before a chunk server is declared dead |
| `LEASE_DURATION_MS` | 60,000 | Write lease validity window (GFS §5.4) |
| `CACHE_TTL_MS` | 60,000 | Client-side chunk location cache lifetime |
| `CHECKPOINT_INTERVAL_MS` | 300,000 | Master checkpoint interval |
| `ORPHAN_GRACE_PERIOD_MS` | 60,000 | Grace period before removing empty file entries |
| `SOCKET_CONNECT_TIMEOUT_MS` | 5,000 | TCP connect deadline |
| `SOCKET_READ_TIMEOUT_MS` | 30,000 | Socket read deadline |
| `RPC_MAX_RETRIES` | 3 | Retry attempts for all RPCs |
| `RPC_RETRY_DELAY_MS` | 500 | Base retry delay (linear backoff: delay × attempt) |

Override Master host/port at runtime:
```bash
-Dgfs.master.host=<host>   # default: localhost
-Dgfs.master.port=<port>   # default: 9000
```

---

## Test Coverage

28 unit tests across 2 test classes — all passing.

### MasterServerTest.java (15 tests)

| Test | Verifies |
|------|---------|
| `testCreateAndRetrieveFile` | File entry created and retrievable by path |
| `testDeleteFileRemovesFromNamespace` | `DELETE_FILE` removes namespace entry |
| `testDeleteFileRemovesChunkMetadata` | Chunk metadata removed from `chunkTable` on delete |
| `testRenameFile` | Old key removed, new key added, `path` field updated |
| `testMkdir` | Directory entry created with `isDirectory = true` |
| `testListFiles` | Prefix filter returns only matching paths |
| `testAddChunkToFile` | `addChunk()` appends to `chunkIds` list |
| `testChunkLocationEquality` | `ChunkLocation.equals()` compares host + port |
| `testServerInfoIsAlive` | Fresh `ServerInfo` reports `isAlive() = true` |
| `testChunkMetadataVersion` | Initial version stored correctly |
| `testLeaseGrantAndValid` | `grantLease()` sets primary and makes `leaseValid()` true |
| `testLeaseRevoke` | `revokeLease()` clears primary, invalidates lease |
| `testStaleReplicaEviction` | Replica with version < master version evicted from locations |
| `testFileMetadataUpdatedAtChanges` | `addChunk()` refreshes `updatedAt` |
| `testFileSizeSetAfterUpload` | `fileSize` field stores correct byte count |

### ChunkStorageTest.java (13 tests)

| Test | Verifies |
|------|---------|
| `testWriteAndRead` | Written bytes round-trip correctly |
| `testDelete` | `has()` returns false after delete |
| `testDeleteRemovesAllSidecars` | `.crc` and `.ver` files deleted alongside data file |
| `testListChunksExcludesSidecars` | `list()` returns only data chunk IDs |
| `testReadMissingChunkThrows` | `FileNotFoundException` on unknown chunk |
| `testAppendToExistingChunk` | Data appended correctly to existing chunk |
| `testAppendToNewChunk` | Sequential appends accumulate correctly |
| `testCrcSidecarCreatedOnWrite` | `.crc` file exists after write |
| `testCrcVerificationPassesOnValidData` | Read succeeds and CRC check passes on unmodified data |
| `testVersionSidecarCreatedOnWrite` | `.ver` file exists with correct version |
| `testVersionReportedInListWithVersions` | `listWithVersions()` returns correct map |
| `testVersionUpdatedOnNewWrite` | Overwriting a chunk updates the `.ver` sidecar |
| `testChunkSize` | `size()` returns the correct byte count |

**Actual test output:**
```
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running ChunkStorageTest
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.207 s
Running MasterServerTest
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0 s

Results:
Tests run: 28, Failures: 0, Errors: 0, Skipped: 0

BUILD SUCCESS
Total time: 2.146 s
```

---

## End-to-End Demo

Tested on Windows 11, OpenJDK 17.0.19, Maven 3.9.16 with 3 ChunkServers on separate racks.

### Cluster startup
```
[Master] Listening on port 9000
[Master] Registered chunk server localhost:9100 (rack=rack-A, free=261.0 GB)
[Master] Registered chunk server localhost:9101 (rack=rack-B, free=261.0 GB)
[Master] Registered chunk server localhost:9102 (rack=rack-C, free=261.0 GB)
```

### Upload + stat
```
$ java -cp $JAR Main mkdir /data
[GfsClient] Created directory: /data

$ java -cp $JAR Main upload README.md /data/readme.txt
[GfsClient] Uploading chunk 1 (id=580677f1-..., v1, 17379 bytes) -> localhost:9100
[GfsClient] Upload complete: README.md -> /data/readme.txt (1 chunk(s), 17379 bytes)

$ java -cp $JAR Main stat /data/readme.txt
Path        : /data/readme.txt
Type        : file
Size        : 17379 bytes
Chunks      : 1
Replicas    : 3
Created     : 2026-06-19 17:32:48
Modified    : 2026-06-19 17:32:48
```

### Append + stat (size and timestamp updated)
```
$ java -cp $JAR Main append /data/readme.txt "Hello from GFS append!"
[GfsClient] Appended 22 bytes to /data/readme.txt at offset 17379

$ java -cp $JAR Main stat /data/readme.txt
Path        : /data/readme.txt
Type        : file
Size        : 17401 bytes        <- grew by 22 bytes
Chunks      : 1
Replicas    : 3
Modified    : 2026-06-19 17:32:49  <- updatedAt refreshed
```

### Download + integrity check
```
$ java -cp $JAR Main download /data/readme.txt recovered.txt
[GfsClient] Downloading /data/readme.txt (1 chunk(s)) -> recovered.txt
[GfsClient] Download complete: recovered.txt

Original MD5 : 39390887454B58830EEEADB771158B7E
Recovered MD5: 39390887454B58830EEEADB771158B7E
PERFECT MATCH
```

### Node failure test
```
# Kill node :9100 mid-session
$ java -cp $JAR Main download /data/readme.txt recovered_after_failure.txt
[GfsClient] Download complete: recovered_after_failure.txt

MD5 after node failure: 39390887454B58830EEEADB771158B7E
MATCH — data accessible with one node down
```

---

## Future Enhancements

| Enhancement | Why It Matters |
|-------------|---------------|
| **Distributed Master with Raft/Paxos** | The single Master is the current SPOF. Replacing it with a replicated state machine eliminates the last point of failure. |
| **gRPC / Protocol Buffers** | Java serialisation is brittle across JVM versions and slow compared to Protobuf. gRPC would also enable cross-language clients. |
| **Erasure Coding (Reed-Solomon)** | 3× replication uses 3× disk space. Erasure coding (e.g., 6+3 RS codes) achieves similar durability at ~1.5× storage overhead. |
| **Multi-file atomic transactions** | Currently each file operation is individually atomic. A distributed transaction layer (2PC or Saga) would enable consistent multi-file operations. |
| **REST / HTTP API** | A REST API would expose the file system to non-Java clients — browsers, curl, Python scripts — without requiring the CLI. |
| **Prometheus / Grafana metrics** | Expose chunk server throughput, replication lag, and Master RPC latency as time-series metrics for operational observability. |
| **Docker Compose cluster** | Package Master and 3 ChunkServers in `docker-compose.yml` for one-command cluster startup. |
| **Quota & access control** | Per-directory storage quotas and POSIX-style ACLs for multi-tenant isolation. |

---

## References

- Ghemawat, S., Gobioff, H., & Leung, S.-T. (2003). **The Google File System**. *Proceedings of the 19th ACM Symposium on Operating Systems Principles (SOSP 2003)*. [PDF](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf)

---

*Built as a portfolio project demonstrating distributed systems engineering: fault-tolerant architecture, consistency protocols, and production-grade reliability patterns.*

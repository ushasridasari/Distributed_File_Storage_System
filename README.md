# GFS Java — Distributed File Storage System

## Project Overview

A distributed file storage system built in Java, inspired by the [Google File System paper (Ghemawat et al., 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).

Traditional single-node storage systems fail when files grow beyond one machine's capacity or when the storage node crashes. GFS solves this by:
- **Splitting files** into fixed-size chunks distributed across many servers
- **Replicating** every chunk on 3 independent nodes so data survives hardware failure
- **Centralising metadata** on a single Master so clients always know where data lives

This implementation covers all core GFS paper responsibilities: chunk leases, version-based stale replica detection, atomic record append, operation log + checkpoint recovery, background re-replication, CRC integrity checks, rack-aware placement, and node rejoin reconciliation.

---

## System Architecture

```
  ┌───────────────────────────────────────────────────────────────┐
  │                        GfsClient                              │
  │  (CLI via Main.java — upload/download/append/stat/delete/…)  │
  └────────────┬──────────────────────────────────┬──────────────┘
               │ metadata RPCs                    │ direct chunk I/O
               ▼                                  ▼
  ┌────────────────────────────┐   ┌──────────────────────────────────────────┐
  │       MasterServer         │   │            ChunkServers                  │
  │  ─────────────────────     │◄──│  :9100          :9101          :9102     │
  │  • file namespace          │   │  data           data           data      │
  │  • chunk→location map      │   │  .crc (CRC32)   .crc           .crc     │
  │  • version tracking        │   │  .ver (version) .ver           .ver     │
  │  • lease management        │   └──────────────────────────────────────────┘
  │  • operation log           │              ▲
  │  • checkpoint/recovery     │              │ heartbeat every 5s
  │  • re-replication          │              │ (chunkId → version map,
  │  • orphan cleanup          │──────────────┘  free disk, rack ID)
  └────────────────────────────┘
```

**Data flow — upload:**
```
Client → Master: CREATE_FILE
Client → Master: REQUEST_CHUNK_WRITE  (lease + replica locations)
Client → ChunkServer primary: WRITE_CHUNK
         primary → secondary1: REPLICATE_CHUNK
         primary → secondary2: REPLICATE_CHUNK
Client → Master: UPDATE_FILE_SIZE
```

**Data flow — download:**
```
Client → Master: GET_FILE_INFO  (chunk list)
Client → Master: REQUEST_CHUNK_READ  (live replica locations, cached 60s)
Client → ChunkServer (any live replica): READ_CHUNK  (CRC verified)
         → fallback to next replica on failure
```

---

## Project Structure

```
src/
├── main/java/
│   ├── Main.java          — CLI entry point (11 commands)
│   ├── MasterServer.java  — metadata coordinator:
│   │                          namespace, chunk registry, chunk versions,
│   │                          stale replica eviction, actual chunk GC,
│   │                          lease grant/expiry/renewal, operation log,
│   │                          checkpoint + log replay, re-replication,
│   │                          node rejoin reconciliation, orphan cleanup
│   ├── ChunkServer.java   — chunk storage:
│   │                          flat files + .crc (CRC32) + .ver (version) sidecars,
│   │                          read/write/append/verify, replication chaining,
│   │                          heartbeat with version map, lease renewal scheduler,
│   │                          rack ID + free disk reporting
│   ├── GfsClient.java     — high-level client API:
│   │                          streaming upload, download, append, delete, rename,
│   │                          mkdir, list, stat, clusterStatus,
│   │                          chunk location cache, replica fallback,
│   │                          RPC retry with linear backoff, socket timeouts
│   ├── GfsConfig.java     — cluster constants + .gfs/config reader/writer
│   └── Message.java       — TCP wire message (type + key-value payload)
└── test/java/
    ├── MasterServerTest.java   — 13 tests: namespace CRUD, rename, mkdir,
    │                             chunk versioning, stale eviction, lease lifecycle,
    │                             chunk GC, fileSize tracking
    └── ChunkStorageTest.java   — 12 tests: write/read, delete + sidecar cleanup,
                                  append, CRC creation + verification,
                                  version sidecar, listWithVersions, chunk size
```

---

## Components

| File | Responsibility |
|------|----------------|
| **MasterServer.java** | Single metadata coordinator. Holds the file namespace and chunk-to-location map as inner classes (`FileMetadata`, `ChunkMetadata`, `ChunkLocation`, `ServerInfo`, `OperationLog`, `Checkpoint`). Handles all RPCs, writes an operation log, takes periodic checkpoints, evicts stale chunk replicas via version numbers, sends `DELETE_CHUNK` RPCs on file delete, manages lease grants/expiry/renewal, re-replicates under-replicated chunks, reconciles rejoining nodes, and removes orphaned file entries. |
| **ChunkServer.java** | Stores chunk data as flat files with `.crc` (CRC32) and `.ver` (version) sidecars. Verifies CRC on every read. Reports `chunkId → version` map plus free disk and rack ID in heartbeats. Retries replication and lease renewals with linear backoff. Lease revocation on transient network error is suppressed — only an explicit Master rejection revokes a lease. |
| **GfsClient.java** | High-level API for all file operations. Streams uploads one 64 MB chunk at a time (O(1) heap). Reports file size to Master after upload and after each append. Falls back across replicas on read errors, evicting stale cache entries. All sockets carry explicit connect (5 s) and read (30 s) timeouts. Master RPCs and chunk writes retry up to 3 times with linear backoff. |
| **GfsConfig.java** | All cluster-wide constants. Reads/writes a `.gfs/config` INI file so constants can be overridden without recompiling. |
| **Message.java** | Lightweight serialisable wire message: a `String type` and a `HashMap<String, Object>` payload. Static helpers `Message.ok()` and `Message.error(String)`. |
| **Main.java** | Single CLI entry point dispatching all 11 commands via a switch-case. |

---

## GFS Design Decisions Implemented

| Decision | GFS Paper § | Detail |
|----------|-------------|--------|
| **64 MB chunk size** | §2.6 | `GfsConfig.CHUNK_SIZE_BYTES` |
| **3× replication** | §2.6 | `GfsConfig.REPLICATION_FACTOR` — each chunk on 3 servers |
| **Primary-chain replication** | §3.1 | Client → primary → secondaries |
| **Lease-based primary election** | §5.4 | 60 s lease (`LEASE_DURATION_MS`); ChunkServer renews 10 s before expiry |
| **Lease expiry + renewal** | §5.4 | `RENEW_LEASE` RPC; Master revokes expired leases and re-elects primary |
| **Atomic record append** | §3.3 | `append` targets the last chunk's primary; new chunk allocated if needed |
| **Chunk version numbers** | §4.5 | Version incremented on each write; `.ver` sidecar on disk |
| **Stale replica eviction** | §4.5 | Heartbeat carries `chunkId → version` map; Master removes replicas with old versions |
| **Actual chunk GC** | §4.4 | `DELETE_FILE` sends `DELETE_CHUNK` RPC to every replica, freeing disk space |
| **Heartbeat-driven registry** | §4.4 | Chunk server locations rebuilt from heartbeats every 5 s |
| **Background re-replication** | §4.4 | Master checks replica counts every 10 s and copies chunks to new servers |
| **Operation log** | §5.2 | Every mutation appended to `.gfs/master.log` before being applied |
| **Namespace checkpoint** | §5.2 | Namespace + chunkTable serialised to `.gfs/checkpoint.ser` every 5 min; log truncated after; loaded on startup with log replay |
| **CRC32 checksums** | §5.2 | `.crc` sidecar per chunk; verified on every read to catch silent corruption |
| **Client-side location cache** | §3.1 | Chunk locations cached for 60 s (`CACHE_TTL_MS`) to reduce Master load |
| **Replica fallback on read** | §3.1 | Client tries next replica if one fails; evicts stale cache entry |
| **Disk-space-aware placement** | §3.1 | ChunkServers report free disk bytes in every heartbeat; Master sorts candidates by free space descending |
| **Rack-aware placement** | §3.1 | Each ChunkServer declares a rack ID; Master fills one slot per rack before reusing any rack |
| **Streaming large-file upload** | §2.6 | One 64 MB buffer at a time — heap usage is O(1) regardless of file size |
| **Accurate file metadata** | §2.6 | `fileSize` and `updatedAt` updated after upload (`UPDATE_FILE_SIZE`) and after every append (`NOTIFY_APPEND`); `stat` reports only live chunk count |
| **Socket timeouts** | §4 | All TCP sockets have explicit connect (5 s) and read (30 s) deadlines |
| **RPC retry with backoff** | §4 | Master RPCs, chunk writes, replication, and lease renewals retry up to 3 times with 500 ms × attempt linear backoff |
| **Safe lease renewal** | §5.4 | Transient network errors do not revoke the lease — only an explicit Master `ERROR` response does |
| **Node rejoin reconciliation** | §4.4, §4.5 | On heartbeat: valid replicas re-admitted immediately; orphaned chunks (file deleted while offline) GC'd via `DELETE_CHUNK` |
| **Orphaned file cleanup** | §4.4 | File entries created but never written are removed after `ORPHAN_GRACE_PERIOD_MS` (60 s) |

---

## Quick Start

### Prerequisites
- Java 11+
- Maven 3.6+

### Build

```bash
mvn clean package -q
JAR=target/gfs-java-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Run

**1. Start the Master:**
```bash
java -cp $JAR Main master
# [Master] Listening on port 9000
```

**2. Start 3 chunk servers (one per rack for full rack-aware replication):**
```bash
java -cp $JAR Main chunkserver 9100 chunk_data/node1 rack-A
java -cp $JAR Main chunkserver 9101 chunk_data/node2 rack-B
java -cp $JAR Main chunkserver 9102 chunk_data/node3 rack-C
```

**3. Use the CLI:**
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

### Run Tests

```bash
mvn test
```

---

## CLI Commands

### File Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `upload` | `upload <local-path> <gfs-path>` | Stream local file into GFS as 64 MB chunks; handles files of any size |
| `download` | `download <gfs-path> <local-path>` | Reassemble chunks from any live replica; falls back across replicas on failure |
| `append` | `append <gfs-path> <text>` | Atomically append text to an existing GFS file (GFS §3.3) |
| `delete` | `delete <gfs-path>` | Remove file and immediately GC all chunk replicas from every server |
| `rename` | `rename <src-path> <dst-path>` | Rename or move a file within the namespace |
| `mkdir` | `mkdir <gfs-path>` | Create a directory entry in the namespace |
| `list` | `list [prefix]` | List all files/directories under a path prefix |
| `stat` | `stat <gfs-path>` | Show type, size, live chunk count, replica count, and timestamps |

### Cluster

| Command | Usage | Description |
|---------|-------|-------------|
| `cluster-status` | `cluster-status` | Show live/dead servers with rack ID and free disk, plus total files and chunks |

### Server Startup

| Command | Usage | Description |
|---------|-------|-------------|
| `master` | `master [port]` | Start the Master server (default port: 9000) |
| `chunkserver` | `chunkserver <port> [storage-dir] [rack-id]` | Start a Chunk server; rack-id groups servers for rack-aware placement (default: `default-rack`) |

---

## Configuration

All defaults live in `GfsConfig.java` and are written to `.gfs/config` on first run:

| Constant | Default | Description |
|----------|---------|-------------|
| `CHUNK_SIZE_BYTES` | 67108864 (64 MB) | Max size of one chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `MASTER_PORT` | 9000 | Master server TCP port |
| `CHUNK_SERVER_BASE_PORT` | 9100 | Default first chunk server port |
| `HEARTBEAT_INTERVAL_MS` | 5000 | How often chunk servers ping the Master |
| `CHUNK_SERVER_TIMEOUT_MS` | 15000 | Time before a chunk server is declared dead |
| `LEASE_DURATION_MS` | 60000 | How long a write lease is valid (GFS §5.4) |
| `CACHE_TTL_MS` | 60000 | Client-side chunk location cache lifetime |
| `CHECKPOINT_INTERVAL_MS` | 300000 | How often the Master checkpoints namespace to disk |
| `ORPHAN_GRACE_PERIOD_MS` | 60000 | Grace period before removing empty file entries |
| `SOCKET_CONNECT_TIMEOUT_MS` | 5000 | TCP connect deadline; prevents hanging on unreachable hosts |
| `SOCKET_READ_TIMEOUT_MS` | 30000 | Socket read deadline; prevents blocking on hung servers |
| `RPC_MAX_RETRIES` | 3 | Retry attempts for Master RPCs, chunk writes, and replication |
| `RPC_RETRY_DELAY_MS` | 500 | Base retry delay (multiplied by attempt number — linear backoff) |

### System Properties

```bash
-Dgfs.master.host=<host>   # override Master host (default: localhost)
-Dgfs.master.port=<port>   # override Master port (default: 9000)
```

---

## Test Coverage

### Unit Tests — MasterServer (`MasterServerTest.java`)

| Test | What it verifies |
|------|-----------------|
| `testCreateAndRetrieveFile` | File entry created in namespace and retrievable by path |
| `testDeleteFileRemovesFromNamespace` | `DELETE_FILE` removes the namespace entry |
| `testDeleteFileRemovesChunkMetadata` | Chunk metadata removed from `chunkTable` on file delete |
| `testRenameFile` | Old key removed, new key added, `path` field updated |
| `testMkdir` | Directory entry created with `isDirectory = true` |
| `testListFiles` | Prefix filter returns only matching paths |
| `testAddChunkToFile` | `addChunk()` appends to `chunkIds` list |
| `testChunkLocationEquality` | `ChunkLocation.equals()` compares host + port |
| `testServerInfoIsAlive` | Fresh `ServerInfo` reports `isAlive() = true` |
| `testChunkMetadataVersion` | Initial version stored correctly |
| `testLeaseGrantAndValid` | `grantLease()` sets primary and makes `leaseValid()` true |
| `testLeaseRevoke` | `revokeLease()` clears primary and invalidates lease |
| `testStaleReplicaEviction` | Server with reported version < master version evicted from locations |
| `testFileMetadataUpdatedAtChanges` | `addChunk()` refreshes `updatedAt` |
| `testFileSizeSetAfterUpload` | `fileSize` field stores correct byte count |

### Unit Tests — ChunkStorage (`ChunkStorageTest.java`)

| Test | What it verifies |
|------|-----------------|
| `testWriteAndRead` | Written bytes round-trip correctly |
| `testDelete` | `has()` returns false after delete |
| `testDeleteRemovesAllSidecars` | `.crc` and `.ver` files deleted alongside data file |
| `testListChunksExcludesSidecars` | `list()` returns only data chunk IDs |
| `testReadMissingChunkThrows` | `FileNotFoundException` thrown for unknown chunk |
| `testAppendToExistingChunk` | Data appended correctly to existing chunk |
| `testAppendToNewChunk` | Sequential appends to a new chunk accumulate correctly |
| `testCrcSidecarCreatedOnWrite` | `.crc` file exists after write |
| `testCrcVerificationPassesOnValidData` | Read succeeds and CRC check passes on unmodified data |
| `testVersionSidecarCreatedOnWrite` | `.ver` file exists and stores correct version |
| `testVersionReportedInListWithVersions` | `listWithVersions()` returns correct map |
| `testVersionUpdatedOnNewWrite` | Overwriting a chunk updates the `.ver` sidecar |
| `testChunkSize` | `size()` returns the correct byte count |

### Running Tests

```bash
mvn test

# Expected output:
# Tests run: 28, Failures: 0, Errors: 0, Skipped: 0
```

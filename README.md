# GFS Java — Distributed File Storage System

A distributed file storage system inspired by the [Google File System paper (Ghemawat et al., 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf), implemented in Java.

## Architecture

```
┌──────────────────┐          ┌────────────────────────────────────────┐
│    GfsClient     │─── RPC ──│             MasterServer               │
│  (Main.java CLI) │          │  namespace + chunk registry +          │
└────────┬─────────┘          │  operation log + checkpoint +          │
         │ direct             │  lease management + re-replication     │
         │ chunk I/O          └────────────────────────────────────────┘
         ▼                              ▲  heartbeat (versions, every 5s)
┌─────────────────────────────────────────────────────┐
│  ChunkServer :9100  │  ChunkServer :9101  │  :9102  │
│  data + .crc + .ver │  data + .crc + .ver │  ...    │
└─────────────────────────────────────────────────────┘
```

## Project Structure

```
src/
├── main/java/
│   ├── Main.java          — CLI entry point (11 commands)
│   ├── MasterServer.java  — metadata coordinator:
│   │                          namespace, chunk registry, chunk versions,
│   │                          stale replica eviction, actual chunk GC,
│   │                          lease grant/expiry/renewal, operation log,
│   │                          checkpoint + log replay, re-replication
│   ├── ChunkServer.java   — chunk storage:
│   │                          flat files + .crc (CRC32) + .ver (version) sidecars,
│   │                          read/write/append/verify, replication chaining,
│   │                          heartbeat with version map, lease renewal scheduler
│   ├── GfsClient.java     — high-level client API:
│   │                          upload, download, append, delete, rename, mkdir,
│   │                          list, stat, clusterStatus, chunk location cache,
│   │                          replica fallback on read
│   ├── GfsConfig.java     — cluster constants + .gfs/config reader/writer
│   └── Message.java       — TCP wire message (type + key-value payload)
└── test/java/
    ├── MasterServerTest.java   — namespace, rename, mkdir, versioning,
    │                             stale eviction, lease, chunk GC tests
    └── ChunkStorageTest.java   — write, read, append, CRC, version sidecar,
                                  delete, list tests
```

## Components

| File | Responsibility |
|------|----------------|
| **MasterServer.java** | Single metadata coordinator. Holds the file namespace and chunk-to-location map as inner classes (`FileMetadata`, `ChunkMetadata`, `ChunkLocation`, `ServerInfo`, `OperationLog`, `Checkpoint`). Handles all RPCs, writes an operation log, takes periodic checkpoints, evicts stale chunk replicas via version numbers, sends actual `DELETE_CHUNK` RPCs on file delete, manages lease grants/expiry/renewal, and re-replicates under-replicated chunks in the background. |
| **ChunkServer.java** | Stores chunk data as flat files. Writes `.crc` (CRC32) and `.ver` (version number) sidecar files alongside every chunk. Verifies CRC on every read. Reports a `chunkId → version` map in heartbeats so the Master can detect stale replicas. Schedules lease renewal requests before the 60 s window expires. |
| **GfsClient.java** | High-level API for all file operations. Passes the chunk version received from the lease into every write so ChunkServers store the correct version. Falls back across replica list on read errors. Maintains a 60-second client-side chunk location cache. |
| **GfsConfig.java** | All cluster-wide constants including `LEASE_DURATION_MS` and `CHECKPOINT_INTERVAL_MS`. Reads/writes a `.gfs/config` INI file. |
| **Message.java** | Lightweight serialisable wire message: a string `type` plus a `HashMap` payload. |
| **Main.java** | Single CLI entry point dispatching all 11 commands. |

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
| **Lazy chunk GC** (directories/rename) | §4.4 | Renamed/deleted file entries cleaned from namespace; chunk GC runs immediately |

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

**2. Start chunk servers (3 for full replication):**
```bash
java -cp $JAR Main chunkserver 9100 chunk_data/node1
java -cp $JAR Main chunkserver 9101 chunk_data/node2
java -cp $JAR Main chunkserver 9102 chunk_data/node3
```

**3. Use the CLI:**
```bash
# Create a directory
java -cp $JAR Main mkdir /data

# Upload a file
java -cp $JAR Main upload myfile.txt /data/myfile.txt

# Show file metadata
java -cp $JAR Main stat /data/myfile.txt

# Atomically append text to a file
java -cp $JAR Main append /data/myfile.txt "new line of data"

# List files
java -cp $JAR Main list /

# Rename a file
java -cp $JAR Main rename /data/myfile.txt /data/renamed.txt

# Download a file
java -cp $JAR Main download /data/renamed.txt local_copy.txt

# Check cluster health
java -cp $JAR Main cluster-status

# Delete a file (chunks are GC'd immediately from all servers)
java -cp $JAR Main delete /data/renamed.txt
```

### Run Tests

```bash
mvn test
```

## CLI Commands

### File Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `upload` | `upload <local-path> <gfs-path>` | Split local file into 64 MB chunks and upload to GFS |
| `download` | `download <gfs-path> <local-path>` | Reassemble chunks from any live replica; falls back across replicas |
| `append` | `append <gfs-path> <text>` | Atomically append text to an existing GFS file (GFS §3.3) |
| `delete` | `delete <gfs-path>` | Remove file and immediately GC chunks from all servers |
| `rename` | `rename <src-path> <dst-path>` | Rename or move a file within the namespace |
| `mkdir` | `mkdir <gfs-path>` | Create a directory entry in the namespace |
| `list` | `list [prefix]` | List all files/directories under a path prefix |
| `stat` | `stat <gfs-path>` | Show type, size, chunk count, replica count, and timestamps |

### Cluster

| Command | Usage | Description |
|---------|-------|-------------|
| `cluster-status` | `cluster-status` | Show live/dead servers, total files, directories, and chunks |

### Server Startup

| Command | Usage | Description |
|---------|-------|-------------|
| `master` | `master [port]` | Start the Master server (default port: 9000) |
| `chunkserver` | `chunkserver <port> [storage-dir]` | Start a Chunk server (default port: 9100) |

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

## System Properties

```bash
-Dgfs.master.host=<host>   # override Master host (default: localhost)
-Dgfs.master.port=<port>   # override Master port (default: 9000)
```

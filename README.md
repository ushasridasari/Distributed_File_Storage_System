# GFS Java — Distributed File Storage System

A distributed file storage system inspired by the [Google File System paper (Ghemawat et al., 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf), implemented in Java.

## Architecture

```
┌──────────────────┐          ┌────────────────────────────────────┐
│    GfsClient     │─── RPC ──│           MasterServer             │
│  (Main.java CLI) │          │  namespace + chunk registry +      │
└────────┬─────────┘          │  operation log + re-replication    │
         │ direct             └────────────────────────────────────┘
         │ chunk I/O                    ▲  heartbeat (every 5s)
         ▼                              │
┌─────────────────────────────────────────────────────┐
│  ChunkServer :9100  │  ChunkServer :9101  │  :9102  │
│  (data + CRC32)     │  (data + CRC32)     │  ...    │
└─────────────────────────────────────────────────────┘
```

## Project Structure

```
src/
├── main/java/
│   ├── Main.java          — CLI entry point (11 commands)
│   ├── MasterServer.java  — metadata coordinator (namespace, chunk registry,
│   │                        operation log, background re-replication)
│   ├── ChunkServer.java   — chunk storage (flat files + CRC32 checksums,
│   │                        read/write/append/verify, replication chaining)
│   ├── GfsClient.java     — high-level client API with chunk location cache
│   ├── GfsConfig.java     — cluster constants + .gfs/config reader/writer
│   └── Message.java       — TCP wire message (type + key-value payload)
└── test/java/
    ├── MasterServerTest.java   — namespace, rename, mkdir, liveness tests
    └── ChunkStorageTest.java   — write, read, append, CRC, delete tests
```

## Components

| File | Responsibility |
|------|----------------|
| **MasterServer.java** | Single metadata coordinator. Holds the file namespace and chunk-to-location map as inner classes (`FileMetadata`, `ChunkMetadata`, `ChunkLocation`, `ServerInfo`). Handles all client and chunk-server RPCs, writes an operation log for crash recovery, and runs a background thread to re-replicate under-replicated chunks. |
| **ChunkServer.java** | Stores chunk data as flat files on local disk. Inner class `ChunkStorage` writes a `.crc` sidecar alongside every chunk and verifies it on every read. Inner class `Handler` serves read/write/append/verify requests and chains replication to secondaries. Sends periodic heartbeats to the Master. |
| **GfsClient.java** | High-level API for all file operations. Maintains a 60-second client-side chunk location cache to avoid hitting the Master on every read. |
| **GfsConfig.java** | All cluster-wide constants. Also reads/writes a `.gfs/config` INI file (same pattern as `Config.java` in the reference DVCS project). |
| **Message.java** | Lightweight serialisable wire message: a string `type` plus a `HashMap` payload. |
| **Main.java** | Single CLI entry point dispatching all 11 commands (same pattern as `Main.java` in the reference DVCS project). |

## GFS Design Decisions Implemented

| Decision | Detail |
|----------|--------|
| **64 MB chunk size** | `GfsConfig.CHUNK_SIZE_BYTES` — same default as the GFS paper |
| **3× replication** | `GfsConfig.REPLICATION_FACTOR` — each chunk written to 3 servers |
| **Primary-chain replication** | Client writes to the primary; primary chains to secondaries |
| **Lease-based primary election** | Master picks the first server as primary when granting a write lease |
| **Atomic record append** | `append` command targets the last chunk's primary (GFS §3.3) |
| **Heartbeat-driven registry** | Chunk server locations rebuilt from heartbeats every 5 s |
| **Background re-replication** | Master checks chunk replica counts every 10 s and fixes under-replicated chunks |
| **Operation log** | Every mutation (create, delete, rename, mkdir, chunk alloc) appended to `.gfs/master.log` |
| **CRC32 checksums** | `.crc` sidecar stored beside every chunk; verified on every read to detect silent corruption |
| **Client-side location cache** | Chunk locations cached for 60 s (`GfsConfig.CACHE_TTL_MS`) to reduce Master load |
| **Lazy chunk GC** | Deleted file's chunk metadata removed from Master; chunk servers clean up on next heartbeat cycle |

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

# List files
java -cp $JAR Main list /

# Show file metadata
java -cp $JAR Main stat /data/myfile.txt

# Atomically append text to a file
java -cp $JAR Main append /data/myfile.txt "new line of data"

# Rename a file
java -cp $JAR Main rename /data/myfile.txt /data/renamed.txt

# Download a file
java -cp $JAR Main download /data/renamed.txt local_copy.txt

# Check cluster health
java -cp $JAR Main cluster-status

# Delete a file
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
| `download` | `download <gfs-path> <local-path>` | Reassemble chunks from any live replica and write locally |
| `append` | `append <gfs-path> <text>` | Atomically append text to an existing GFS file (GFS §3.3) |
| `delete` | `delete <gfs-path>` | Remove file from namespace and schedule chunk GC |
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

All defaults live in `GfsConfig.java` and are also written to `.gfs/config` on first run:

| Constant | Default | Description |
|----------|---------|-------------|
| `CHUNK_SIZE_BYTES` | 67108864 (64 MB) | Max size of one chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `MASTER_PORT` | 9000 | Master server TCP port |
| `CHUNK_SERVER_BASE_PORT` | 9100 | Default first chunk server port |
| `HEARTBEAT_INTERVAL_MS` | 5000 | How often chunk servers ping the Master |
| `CHUNK_SERVER_TIMEOUT_MS` | 15000 | Time before a chunk server is declared dead |
| `CACHE_TTL_MS` | 60000 | Client-side chunk location cache lifetime |

## System properties

```bash
-Dgfs.master.host=<host>   # override Master host (default: localhost)
-Dgfs.master.port=<port>   # override Master port (default: 9000)
```

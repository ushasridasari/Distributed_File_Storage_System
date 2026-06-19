# GFS Java — Distributed File Storage System

A distributed file storage system inspired by the [Google File System paper (Ghemawat et al., 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf), implemented in Java.

## Architecture

```
┌─────────────┐         ┌──────────────────┐
│  GFS Client │──────── │  Master Server   │
│  (GfsCli)   │  RPC    │  (Namespace +    │
└──────┬──────┘         │   ChunkRegistry) │
       │ direct         └──────────────────┘
       │ chunk I/O              ▲ heartbeat
       ▼                        │
┌──────────────────────────────────────────┐
│  ChunkServer 1 │ ChunkServer 2 │ CS 3 …  │
│  (port 9100)   │  (port 9101)  │         │
└──────────────────────────────────────────┘
```

### Components

| Component | Class | Responsibility |
|-----------|-------|----------------|
| **MasterServer** | `com.gfs.master.MasterServer` | Single metadata coordinator. Manages file namespace, chunk locations, and lease grants. |
| **Namespace** | `com.gfs.master.Namespace` | In-memory file-path → FileMetadata mapping (like GFS's in-memory namespace tree). |
| **ChunkServerRegistry** | `com.gfs.master.ChunkServerRegistry` | Tracks live chunk servers via heartbeats; selects placement targets for new chunks. |
| **ChunkServer** | `com.gfs.chunkserver.ChunkServer` | Stores chunk data as flat files. Registers with Master and sends periodic heartbeats. |
| **ChunkStorage** | `com.gfs.chunkserver.ChunkStorage` | Local disk abstraction: one file per chunk, named by chunk UUID. |
| **GfsClient** | `com.gfs.client.GfsClient` | High-level API: upload/download with automatic chunking and replica-aware reads. |
| **GfsCli** | `com.gfs.cli.GfsCli` | Command-line shell around `GfsClient`. |

### GFS Design Decisions Implemented

- **64 MB chunk size** — same as the original paper (`GfsConfig.CHUNK_SIZE_BYTES`)
- **3× replication** — each chunk written to 3 chunk servers (`GfsConfig.REPLICATION_FACTOR`)
- **Primary-chain replication** — client writes to the primary; primary chains to secondaries
- **Lease-based primary election** — Master assigns the first location as primary when granting a write lease
- **Heartbeat-driven metadata** — chunk-server locations rebuilt from heartbeats, not persisted
- **Lazy chunk GC** — deleted file's chunk metadata removed from Master; servers clean up on next cycle

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
java -cp $JAR com.gfs.master.MasterServer
# listening on port 9000
```

**2. Start chunk servers (3 for full replication):**
```bash
java -cp $JAR com.gfs.chunkserver.ChunkServer 9100 chunk_data/node1
java -cp $JAR com.gfs.chunkserver.ChunkServer 9101 chunk_data/node2
java -cp $JAR com.gfs.chunkserver.ChunkServer 9102 chunk_data/node3
```

**3. Use the CLI:**
```bash
# Upload a file
java -cp $JAR com.gfs.cli.GfsCli upload myfile.txt /data/myfile.txt

# List files
java -cp $JAR com.gfs.cli.GfsCli list /

# Download a file
java -cp $JAR com.gfs.cli.GfsCli download /data/myfile.txt myfile_copy.txt

# Delete a file
java -cp $JAR com.gfs.cli.GfsCli delete /data/myfile.txt
```

### Run Tests

```bash
mvn test
```

## Configuration

All defaults are in `GfsConfig.java`:

| Property | Default | Description |
|----------|---------|-------------|
| `CHUNK_SIZE_BYTES` | 67108864 (64 MB) | Max size of one chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `MASTER_PORT` | 9000 | Master server TCP port |
| `CHUNK_SERVER_BASE_PORT` | 9100 | First chunk server port |
| `HEARTBEAT_INTERVAL_MS` | 5000 | How often chunk servers ping the Master |
| `CHUNK_SERVER_TIMEOUT_MS` | 15000 | Time before a chunk server is considered dead |

## Project Structure

```
src/
├── main/java/com/gfs/
│   ├── common/          # Shared models: Message, FileMetadata, ChunkMetadata, ChunkLocation, GfsConfig
│   ├── master/          # MasterServer, Namespace, ChunkServerRegistry, MasterHandler
│   ├── chunkserver/     # ChunkServer, ChunkStorage, ChunkServerHandler
│   ├── client/          # GfsClient, MasterClient, ChunkClient
│   └── cli/             # GfsCli
└── test/java/com/gfs/
    ├── NamespaceTest.java
    └── ChunkStorageTest.java
```

## What's Next

- [ ] Operation log + checkpoint for Master crash recovery
- [ ] Shadow master for high availability
- [ ] Chunk version numbers and stale replica detection
- [ ] Client-side caching of chunk locations
- [ ] End-to-end checksums (CRC32 per chunk)
- [ ] gRPC transport instead of Java serialization

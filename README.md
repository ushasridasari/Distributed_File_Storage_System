# Distributed File Storage System

A distributed file storage system built in Python, featuring chunk-based storage, automatic replication, and a CLI client — inspired by the HDFS NameNode/DataNode architecture.

---

## Architecture

```
┌─────────────┐        ┌──────────────────┐
│   CLI /     │        │  Metadata Server │
│  DFSClient  │◄──────►│  (port 9000)     │
└─────────────┘        │  - file registry │
                       │  - chunk map     │
                       │  - node registry │
                       └────────┬─────────┘
                                │ heartbeat
               ┌────────────────┼────────────────┐
               ▼                ▼                ▼
        ┌────────────┐  ┌────────────┐  ┌────────────┐
        │ Storage    │  │ Storage    │  │ Storage    │
        │ Node :9001 │  │ Node :9002 │  │ Node :9003 │
        └────────────┘  └────────────┘  └────────────┘
```

**Metadata Server** acts as the coordinator (like HDFS NameNode):
- Tracks which files exist and where their chunks live
- Assigns chunk placement using least-used-node load balancing
- Evicts nodes that miss heartbeats for more than 15 seconds

**Storage Nodes** store raw chunks on disk (like HDFS DataNodes):
- Register with the metadata server on startup
- Send heartbeats every 5 seconds with current usage stats
- Serve chunks via HTTP PUT/GET/DELETE

**Client** splits files into 4 MB chunks, uploads them with replication, and reassembles them on download with MD5 checksum verification.

---

## Features

- **Chunk-based storage** — files split into 4 MB chunks
- **Automatic replication** — each chunk stored on 2 nodes (configurable)
- **MD5 integrity checks** — every chunk verified on upload and download
- **Load-balanced placement** — chunks routed to nodes with least bytes stored
- **Heartbeat health monitoring** — stale nodes automatically evicted
- **Colorful CLI** — progress bars, tabular output, colored status indicators
- **REST API** — metadata server and storage nodes expose clean HTTP endpoints

---

## Project Structure

```
.
├── dfs/
│   ├── config.py           # Global constants (chunk size, replication, ports)
│   ├── metadata_server.py  # Flask app — coordinator node
│   ├── storage_node.py     # Flask app — data node
│   ├── client.py           # DFSClient class
│   └── cli.py              # Click-based CLI
├── run_metadata_server.py  # Launcher for the metadata server
├── run_storage_node.py     # Launcher for a storage node
├── demo.py                 # End-to-end demo (uploads a 9 MB file and verifies)
└── requirements.txt
```

---

## Installation

**Requirements:** Python 3.8+

```bash
# Clone the repo
git clone https://github.com/ushasridasari/Distributed_File_Storage_System.git
cd Distributed_File_Storage_System

# Install dependencies
pip install -r requirements.txt
```

---

## Quick Start

### 1. Start the metadata server

```bash
python run_metadata_server.py
# Listening on http://127.0.0.1:9000
```

### 2. Start two or more storage nodes

```bash
# Terminal 2
python run_storage_node.py --port 9001 --storage-dir ./data/node1

# Terminal 3
python run_storage_node.py --port 9002 --storage-dir ./data/node2

# Terminal 4 (optional third node)
python run_storage_node.py --port 9003 --storage-dir ./data/node3
```

### 3. Use the CLI

```bash
# Upload a file
python -m dfs.cli upload ./my_file.pdf /documents/my_file.pdf

# List stored files
python -m dfs.cli ls

# Download a file
python -m dfs.cli download /documents/my_file.pdf ./downloaded.pdf

# Show chunk-level info for a file
python -m dfs.cli info /documents/my_file.pdf

# List storage nodes and their health
python -m dfs.cli nodes

# Check metadata server health
python -m dfs.cli health

# Delete a file
python -m dfs.cli delete /documents/my_file.pdf
```

> By default the CLI connects to `127.0.0.1:9000`. Override with `--host` and `--port`:
> ```bash
> python -m dfs.cli --host 192.168.1.10 --port 9000 ls
> ```

---

## Run the Demo

The included demo starts everything in-process, uploads a 9 MB random file, lists files and nodes, downloads, verifies the MD5, and cleans up — all in one command:

```bash
python demo.py
```

Expected output:

```
============================================================
  Distributed File Storage System — Demo
============================================================

[demo] Starting metadata server on port 9100...
[demo] Metadata server ready.
[demo] Starting storage node on port 9101...
[demo] Starting storage node on port 9102...

[demo] Health check...
  status=ok  nodes=2  files=0

[demo] Creating 9 MB test file...
  MD5: d41d8cd98f00b204e9800998ecf8427e

[demo] Uploading /data/bigfile.bin ...
  Uploaded: 3 chunk(s)

...

[demo] MD5 match: True

============================================================
  All checks passed!
============================================================
```

---

## Configuration

Edit `dfs/config.py` to tune system behaviour:

| Setting | Default | Description |
|---|---|---|
| `CHUNK_SIZE` | `4 MB` | Size of each file chunk |
| `REPLICATION_FACTOR` | `2` | Number of copies per chunk |
| `HEARTBEAT_INTERVAL` | `5 s` | How often storage nodes ping the metadata server |
| `NODE_TIMEOUT` | `15 s` | Time before a silent node is evicted |
| `METADATA_SERVER_HOST` | `127.0.0.1` | Metadata server bind address |
| `METADATA_SERVER_PORT` | `9000` | Metadata server port |

---

## API Reference

### Metadata Server (`http://127.0.0.1:9000`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Server health and counts |
| `GET` | `/nodes` | List registered storage nodes |
| `POST` | `/nodes/register` | Register a storage node |
| `POST` | `/nodes/heartbeat` | Node heartbeat |
| `GET` | `/files` | List all files |
| `POST` | `/files/upload/init` | Initiate upload (returns chunk assignments) |
| `POST` | `/files/upload/commit` | Commit upload after chunks are stored |
| `GET` | `/files/download` | Get chunk locations for download |
| `DELETE` | `/files/delete` | Delete a file |

### Storage Node (`http://127.0.0.1:<port>`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Node health |
| `GET` | `/chunks` | List stored chunk IDs |
| `PUT` | `/chunk/<chunk_id>` | Store a chunk |
| `GET` | `/chunk/<chunk_id>` | Retrieve a chunk |
| `DELETE` | `/chunk/<chunk_id>` | Delete a chunk |

---

## Technology Stack

| Component | Library |
|---|---|
| HTTP servers | Flask |
| HTTP client | requests |
| CLI framework | Click |
| Colored output | colorama |
| Table formatting | tabulate |

---

## Known Limitations

- **In-memory metadata** — the metadata server state is lost on restart; no persistence layer yet
- **Module-level storage node globals** — running two nodes in the same Python process can cause race conditions (use separate processes in production)
- **No TLS** — all traffic is plain HTTP; add a reverse proxy (nginx/caddy) for production deployments
- **No authentication** — any client that can reach port 9000 can read or delete files

---

## License

MIT

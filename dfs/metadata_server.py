"""
Metadata Server — the brain of the distributed file system.

Responsibilities:
- Register / deregister storage nodes
- Accept heartbeats; mark nodes dead when they stop
- Store the file registry: path -> chunk list with locations
- Assign storage nodes for chunk uploads (load-balanced)
- Answer client queries for chunk locations on download
"""

import time
import threading
import hashlib
import uuid
from flask import Flask, request, jsonify

from dfs.config import (
    REPLICATION_FACTOR,
    NODE_TIMEOUT,
    HEARTBEAT_INTERVAL,
    METADATA_SERVER_HOST,
    METADATA_SERVER_PORT,
)

app = Flask(__name__)

# --------------------------------------------------------------------------- #
# In-memory state                                                               #
# --------------------------------------------------------------------------- #

# node_id -> {host, port, last_heartbeat, chunks_stored, bytes_stored}
nodes: dict[str, dict] = {}
nodes_lock = threading.Lock()

# remote_path -> {size, chunk_size, num_chunks,
#                 chunks: [{chunk_id, index, checksum, nodes: [node_id, ...]}]}
files: dict[str, dict] = {}
files_lock = threading.Lock()

# chunk_id -> [node_id, ...]  (reverse index for fast lookup)
chunk_locations: dict[str, list[str]] = {}
chunk_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# Background: evict dead nodes                                                  #
# --------------------------------------------------------------------------- #

def _reaper():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        with nodes_lock:
            dead = [nid for nid, info in nodes.items()
                    if now - info["last_heartbeat"] > NODE_TIMEOUT]
            for nid in dead:
                print(f"[metadata] node dead: {nid}")
                del nodes[nid]


threading.Thread(target=_reaper, daemon=True).start()


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _live_nodes() -> list[str]:
    with nodes_lock:
        return list(nodes.keys())


def _pick_nodes(n: int) -> list[str]:
    """Return n node IDs with the least bytes stored (round-robin fallback)."""
    with nodes_lock:
        alive = sorted(nodes.items(), key=lambda kv: kv[1]["bytes_stored"])
        return [nid for nid, _ in alive[:n]]


def _node_info(node_id: str) -> dict | None:
    with nodes_lock:
        return nodes.get(node_id)


# --------------------------------------------------------------------------- #
# Node management endpoints                                                     #
# --------------------------------------------------------------------------- #

@app.route("/nodes/register", methods=["POST"])
def register_node():
    data = request.json
    node_id = data.get("node_id") or str(uuid.uuid4())
    host = data["host"]
    port = data["port"]

    with nodes_lock:
        nodes[node_id] = {
            "host": host,
            "port": port,
            "last_heartbeat": time.time(),
            "chunks_stored": 0,
            "bytes_stored": 0,
        }
    print(f"[metadata] registered node {node_id} @ {host}:{port}")
    return jsonify({"node_id": node_id, "status": "registered"})


@app.route("/nodes/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json
    node_id = data["node_id"]
    with nodes_lock:
        if node_id not in nodes:
            return jsonify({"status": "unknown_node"}), 404
        nodes[node_id]["last_heartbeat"] = time.time()
        nodes[node_id]["chunks_stored"] = data.get("chunks_stored", 0)
        nodes[node_id]["bytes_stored"] = data.get("bytes_stored", 0)
    return jsonify({"status": "ok"})


@app.route("/nodes", methods=["GET"])
def list_nodes():
    with nodes_lock:
        return jsonify({
            nid: {**info, "last_heartbeat": round(time.time() - info["last_heartbeat"], 1)}
            for nid, info in nodes.items()
        })


# --------------------------------------------------------------------------- #
# File / chunk management endpoints                                             #
# --------------------------------------------------------------------------- #

@app.route("/files/upload/init", methods=["POST"])
def upload_init():
    """
    Client tells us: file path, total size, chunk size, number of chunks.
    We reply with a list of chunk assignments: [{chunk_id, nodes: [{node_id, host, port}]}]
    """
    data = request.json
    remote_path = data["path"]
    total_size = data["size"]
    chunk_size = data["chunk_size"]
    num_chunks = data["num_chunks"]

    alive = _live_nodes()
    if len(alive) < 1:
        return jsonify({"error": "no storage nodes available"}), 503

    replication = min(REPLICATION_FACTOR, len(alive))
    assignments = []

    for i in range(num_chunks):
        chunk_id = hashlib.sha256(f"{remote_path}:{i}:{uuid.uuid4()}".encode()).hexdigest()[:32]
        targets = _pick_nodes(replication)
        node_details = []
        for nid in targets:
            info = _node_info(nid)
            if info:
                node_details.append({"node_id": nid, "host": info["host"], "port": info["port"]})
        assignments.append({"chunk_id": chunk_id, "index": i, "nodes": node_details})

    with files_lock:
        files[remote_path] = {
            "size": total_size,
            "chunk_size": chunk_size,
            "num_chunks": num_chunks,
            "chunks": [],          # filled in by /upload/commit
            "created_at": time.time(),
        }

    return jsonify({"assignments": assignments, "replication": replication})


@app.route("/files/upload/commit", methods=["POST"])
def upload_commit():
    """
    Client tells us which chunks landed on which nodes (with checksums).
    """
    data = request.json
    remote_path = data["path"]
    chunks = data["chunks"]   # [{chunk_id, index, checksum, nodes:[node_id,...]}]

    with files_lock:
        if remote_path not in files:
            return jsonify({"error": "upload not initialised"}), 400
        files[remote_path]["chunks"] = chunks

    with chunk_lock:
        for chunk in chunks:
            chunk_locations[chunk["chunk_id"]] = chunk["nodes"]

    print(f"[metadata] committed {remote_path} ({len(chunks)} chunks)")
    return jsonify({"status": "committed"})


@app.route("/files/download", methods=["GET"])
def download_info():
    """Return chunk locations for a file."""
    remote_path = request.args.get("path")
    with files_lock:
        meta = files.get(remote_path)
    if not meta:
        return jsonify({"error": "file not found"}), 404

    # Attach live node addresses to each chunk
    chunks_with_addrs = []
    for chunk in meta["chunks"]:
        node_details = []
        for nid in chunk.get("nodes", []):
            info = _node_info(nid)
            if info:
                node_details.append({"node_id": nid, "host": info["host"], "port": info["port"]})
        chunks_with_addrs.append({**chunk, "nodes": node_details})

    return jsonify({**meta, "path": remote_path, "chunks": chunks_with_addrs})


@app.route("/files/delete", methods=["DELETE"])
def delete_file():
    remote_path = request.args.get("path")
    with files_lock:
        meta = files.pop(remote_path, None)
    if not meta:
        return jsonify({"error": "file not found"}), 404

    chunk_ids = [c["chunk_id"] for c in meta.get("chunks", [])]
    with chunk_lock:
        for cid in chunk_ids:
            chunk_locations.pop(cid, None)

    # Tell nodes to delete their copies (best-effort)
    for chunk in meta.get("chunks", []):
        for nid in chunk.get("nodes", []):
            info = _node_info(nid)
            if info:
                try:
                    import requests as req
                    req.delete(
                        f"http://{info['host']}:{info['port']}/chunk/{chunk['chunk_id']}",
                        timeout=3,
                    )
                except Exception:
                    pass

    return jsonify({"status": "deleted", "chunks_removed": len(chunk_ids)})


@app.route("/files", methods=["GET"])
def list_files():
    with files_lock:
        result = {
            path: {
                "size": meta["size"],
                "num_chunks": meta["num_chunks"],
                "created_at": meta["created_at"],
            }
            for path, meta in files.items()
        }
    return jsonify(result)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "nodes": len(_live_nodes()), "files": len(files)})


# --------------------------------------------------------------------------- #
# Entry point                                                                   #
# --------------------------------------------------------------------------- #

def run():
    print(f"[metadata] starting on {METADATA_SERVER_HOST}:{METADATA_SERVER_PORT}")
    app.run(host=METADATA_SERVER_HOST, port=METADATA_SERVER_PORT, threaded=True)


if __name__ == "__main__":
    run()

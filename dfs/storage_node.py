"""
Storage Node — stores raw file chunks on disk.

Each node:
- Registers itself with the metadata server on startup
- Sends periodic heartbeats with storage stats
- Exposes HTTP endpoints to PUT / GET / DELETE individual chunks
- Chunks are stored as plain files under <storage_dir>/<chunk_id>
"""

import os
import sys
import time
import uuid
import hashlib
import threading

import requests
from flask import Flask, request, jsonify, send_file, abort

from dfs.config import (
    HEARTBEAT_INTERVAL,
    METADATA_SERVER_HOST,
    METADATA_SERVER_PORT,
)

app = Flask(__name__)

# Set by run()
NODE_ID: str = ""
HOST: str = ""
PORT: int = 0
STORAGE_DIR: str = ""


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _metadata_url(path: str) -> str:
    return f"http://{METADATA_SERVER_HOST}:{METADATA_SERVER_PORT}{path}"


def _chunk_path(chunk_id: str) -> str:
    return os.path.join(STORAGE_DIR, chunk_id)


def _storage_stats() -> tuple[int, int]:
    """Return (chunks_stored, bytes_stored)."""
    total_bytes = 0
    count = 0
    for fname in os.listdir(STORAGE_DIR):
        fpath = os.path.join(STORAGE_DIR, fname)
        if os.path.isfile(fpath):
            total_bytes += os.path.getsize(fpath)
            count += 1
    return count, total_bytes


# --------------------------------------------------------------------------- #
# Heartbeat thread                                                              #
# --------------------------------------------------------------------------- #

def _heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            chunks, byt = _storage_stats()
            requests.post(
                _metadata_url("/nodes/heartbeat"),
                json={"node_id": NODE_ID, "chunks_stored": chunks, "bytes_stored": byt},
                timeout=5,
            )
        except Exception as exc:
            print(f"[node:{PORT}] heartbeat failed: {exc}")


# --------------------------------------------------------------------------- #
# Chunk endpoints                                                               #
# --------------------------------------------------------------------------- #

@app.route("/chunk/<chunk_id>", methods=["PUT"])
def put_chunk(chunk_id: str):
    data = request.data
    if not data:
        return jsonify({"error": "empty body"}), 400

    checksum = hashlib.md5(data).hexdigest()
    path = _chunk_path(chunk_id)
    with open(path, "wb") as f:
        f.write(data)

    return jsonify({"chunk_id": chunk_id, "size": len(data), "checksum": checksum})


@app.route("/chunk/<chunk_id>", methods=["GET"])
def get_chunk(chunk_id: str):
    path = _chunk_path(chunk_id)
    if not os.path.exists(path):
        abort(404)
    return send_file(path, mimetype="application/octet-stream")


@app.route("/chunk/<chunk_id>", methods=["DELETE"])
def delete_chunk(chunk_id: str):
    path = _chunk_path(chunk_id)
    if os.path.exists(path):
        os.remove(path)
        return jsonify({"status": "deleted"})
    return jsonify({"status": "not_found"}), 404


@app.route("/chunks", methods=["GET"])
def list_chunks():
    chunks = []
    for fname in os.listdir(STORAGE_DIR):
        fpath = os.path.join(STORAGE_DIR, fname)
        if os.path.isfile(fpath):
            chunks.append({"chunk_id": fname, "size": os.path.getsize(fpath)})
    return jsonify({"node_id": NODE_ID, "chunks": chunks})


@app.route("/health", methods=["GET"])
def health():
    chunks, byt = _storage_stats()
    return jsonify({
        "node_id": NODE_ID,
        "host": HOST,
        "port": PORT,
        "storage_dir": STORAGE_DIR,
        "chunks_stored": chunks,
        "bytes_stored": byt,
    })


# --------------------------------------------------------------------------- #
# Entry point                                                                   #
# --------------------------------------------------------------------------- #

def run(host: str, port: int, storage_dir: str, node_id: str | None = None):
    global NODE_ID, HOST, PORT, STORAGE_DIR

    HOST = host
    PORT = port
    STORAGE_DIR = storage_dir
    NODE_ID = node_id or str(uuid.uuid4())

    os.makedirs(STORAGE_DIR, exist_ok=True)

    # Register with metadata server
    try:
        resp = requests.post(
            _metadata_url("/nodes/register"),
            json={"node_id": NODE_ID, "host": HOST, "port": PORT},
            timeout=5,
        )
        NODE_ID = resp.json().get("node_id", NODE_ID)
        print(f"[node:{PORT}] registered as {NODE_ID}")
    except Exception as exc:
        print(f"[node:{PORT}] WARNING: could not register with metadata server: {exc}")

    # Start heartbeat thread
    threading.Thread(target=_heartbeat_loop, daemon=True).start()

    print(f"[node:{PORT}] storage dir: {STORAGE_DIR}")
    app.run(host=HOST, port=PORT, threaded=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DFS Storage Node")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--storage-dir", required=True)
    parser.add_argument("--node-id", default=None)
    args = parser.parse_args()

    run(args.host, args.port, args.storage_dir, args.node_id)

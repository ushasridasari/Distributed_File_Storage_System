"""
End-to-end demo of the Distributed File Storage System.

Starts the metadata server and two storage nodes in background threads,
then exercises upload / download / list / delete via the client API.

Run:
  python demo.py
"""

import os
import time
import hashlib
import tempfile
import threading

from dfs import config

# Use test ports so we don't collide with the real servers
config.METADATA_SERVER_PORT = 9100
TEST_NODE_PORTS = [9101, 9102]

from dfs.metadata_server import app as meta_app
from dfs.storage_node import run as node_run, app as node_app
from dfs.client import DFSClient


# --------------------------------------------------------------------------- #
# Launch servers in background threads                                          #
# --------------------------------------------------------------------------- #

def _start_meta():
    meta_app.run(host="127.0.0.1", port=9100, threaded=True, use_reloader=False)


def _start_node(port, storage_dir):
    node_run("127.0.0.1", port, storage_dir)


def _wait_for_server(url, retries=20):
    import requests
    for _ in range(retries):
        try:
            requests.get(url, timeout=1)
            return True
        except Exception:
            time.sleep(0.3)
    return False


# --------------------------------------------------------------------------- #
# Demo                                                                          #
# --------------------------------------------------------------------------- #

def main():
    print("=" * 60)
    print("  Distributed File Storage System — Demo")
    print("=" * 60)

    # Temp dirs for node storage
    tmp = tempfile.mkdtemp(prefix="dfs_demo_")
    node_dirs = [os.path.join(tmp, f"node{i}") for i in range(len(TEST_NODE_PORTS))]

    # Start metadata server
    print("\n[demo] Starting metadata server on port 9100...")
    threading.Thread(target=_start_meta, daemon=True).start()
    assert _wait_for_server("http://127.0.0.1:9100/health"), "Metadata server failed to start"
    print("[demo] Metadata server ready.")

    # Start storage nodes
    for port, storage_dir in zip(TEST_NODE_PORTS, node_dirs):
        print(f"[demo] Starting storage node on port {port}...")
        threading.Thread(target=_start_node, args=(port, storage_dir), daemon=True).start()
        time.sleep(0.5)

    time.sleep(1)

    client = DFSClient(metadata_host="127.0.0.1", metadata_port=9100)

    # ---- health check ----
    print("\n[demo] Health check...")
    h = client.health()
    print(f"  status=ok  nodes={h['nodes']}  files={h['files']}")

    # ---- create a test file ----
    test_file = os.path.join(tmp, "test_upload.bin")
    test_size = 9 * 1024 * 1024   # 9 MB → 3 chunks of 4 MB each
    print(f"\n[demo] Creating {test_size // (1024*1024)} MB test file...")
    data = os.urandom(test_size)
    with open(test_file, "wb") as f:
        f.write(data)
    original_hash = hashlib.md5(data).hexdigest()
    print(f"  MD5: {original_hash}")

    # ---- upload ----
    print("\n[demo] Uploading /data/bigfile.bin ...")
    result = client.upload(test_file, "/data/bigfile.bin")
    print(f"  Uploaded: {result['num_chunks']} chunk(s)")

    # ---- list ----
    print("\n[demo] Listing files...")
    files = client.list_files()
    for path, meta in files.items():
        print(f"  {path}  size={meta['size']}  chunks={meta['num_chunks']}")

    # ---- nodes ----
    print("\n[demo] Storage nodes...")
    node_map = client.list_nodes()
    for nid, info in node_map.items():
        print(f"  {nid[:12]}... @ {info['host']}:{info['port']}  chunks={info['chunks_stored']}")

    # ---- download and verify ----
    download_path = os.path.join(tmp, "test_download.bin")
    print(f"\n[demo] Downloading /data/bigfile.bin ...")
    client.download("/data/bigfile.bin", download_path)
    with open(download_path, "rb") as f:
        downloaded_hash = hashlib.md5(f.read()).hexdigest()
    match = original_hash == downloaded_hash
    print(f"  MD5 match: {match}  (original={original_hash[:8]}...  downloaded={downloaded_hash[:8]}...)")
    assert match, "INTEGRITY CHECK FAILED"

    # ---- delete ----
    print("\n[demo] Deleting /data/bigfile.bin ...")
    result = client.delete("/data/bigfile.bin")
    print(f"  Removed {result['chunks_removed']} chunk(s)")

    files = client.list_files()
    print(f"  Files remaining: {len(files)}")

    print("\n" + "=" * 60)
    print("  All checks passed!")
    print("=" * 60)


if __name__ == "__main__":
    main()

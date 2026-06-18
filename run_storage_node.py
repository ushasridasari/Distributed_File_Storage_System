"""Start a DFS Storage Node.

Usage:
  python run_storage_node.py --port 9001 --storage-dir ./data/node1
  python run_storage_node.py --port 9002 --storage-dir ./data/node2
  python run_storage_node.py --port 9003 --storage-dir ./data/node3
"""
import argparse
from dfs.storage_node import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DFS Storage Node")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--storage-dir", required=True, help="Directory to store chunks")
    parser.add_argument("--node-id", default=None, help="Optional fixed node ID")
    args = parser.parse_args()

    run(args.host, args.port, args.storage_dir, args.node_id)

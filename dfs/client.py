"""
DFS Client library.

Provides upload, download, list, delete, and info operations.
All operations go through the metadata server; chunk data flows
directly between the client and the storage nodes.
"""

import os
import math
import hashlib

import requests

from dfs.config import (
    CHUNK_SIZE,
    METADATA_SERVER_HOST,
    METADATA_SERVER_PORT,
)


class DFSClient:
    def __init__(
        self,
        metadata_host: str = METADATA_SERVER_HOST,
        metadata_port: int = METADATA_SERVER_PORT,
    ):
        self.base_url = f"http://{metadata_host}:{metadata_port}"

    # ------------------------------------------------------------------ #
    # Internal helpers                                                      #
    # ------------------------------------------------------------------ #

    def _meta(self, path: str, **kwargs) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", **kwargs)

    def _meta_post(self, path: str, **kwargs) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", **kwargs)

    @staticmethod
    def _chunk_url(node: dict, chunk_id: str) -> str:
        return f"http://{node['host']}:{node['port']}/chunk/{chunk_id}"

    @staticmethod
    def _checksum(data: bytes) -> str:
        return hashlib.md5(data).hexdigest()

    # ------------------------------------------------------------------ #
    # Upload                                                                #
    # ------------------------------------------------------------------ #

    def upload(self, local_path: str, remote_path: str, progress=None):
        """
        Upload a local file to the DFS.

        progress: optional callable(bytes_done, total_bytes)
        """
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"local file not found: {local_path}")

        total_size = os.path.getsize(local_path)
        num_chunks = max(1, math.ceil(total_size / CHUNK_SIZE))

        # Ask metadata server for chunk assignments
        init_resp = self._meta_post(
            "/files/upload/init",
            json={
                "path": remote_path,
                "size": total_size,
                "chunk_size": CHUNK_SIZE,
                "num_chunks": num_chunks,
            },
            timeout=10,
        )
        init_resp.raise_for_status()
        assignments = init_resp.json()["assignments"]

        committed_chunks = []
        bytes_done = 0

        with open(local_path, "rb") as f:
            for assignment in assignments:
                chunk_id = assignment["chunk_id"]
                index = assignment["index"]
                nodes = assignment["nodes"]

                data = f.read(CHUNK_SIZE)
                if not data:
                    break

                checksum = self._checksum(data)
                uploaded_to = []

                for node in nodes:
                    try:
                        url = self._chunk_url(node, chunk_id)
                        r = requests.put(url, data=data, timeout=30)
                        r.raise_for_status()
                        uploaded_to.append(node["node_id"])
                    except Exception as exc:
                        print(f"  [warn] could not upload chunk {index} to {node['node_id']}: {exc}")

                if not uploaded_to:
                    raise RuntimeError(f"chunk {index} could not be stored on any node")

                committed_chunks.append({
                    "chunk_id": chunk_id,
                    "index": index,
                    "checksum": checksum,
                    "size": len(data),
                    "nodes": uploaded_to,
                })

                bytes_done += len(data)
                if progress:
                    progress(bytes_done, total_size)

        # Commit to metadata server
        commit_resp = self._meta_post(
            "/files/upload/commit",
            json={"path": remote_path, "chunks": committed_chunks},
            timeout=10,
        )
        commit_resp.raise_for_status()

        return {
            "path": remote_path,
            "size": total_size,
            "num_chunks": len(committed_chunks),
        }

    # ------------------------------------------------------------------ #
    # Download                                                              #
    # ------------------------------------------------------------------ #

    def download(self, remote_path: str, local_path: str, progress=None):
        """Download a file from the DFS to a local path."""
        meta_resp = self._meta("/files/download", params={"path": remote_path}, timeout=10)
        if meta_resp.status_code == 404:
            raise FileNotFoundError(f"remote file not found: {remote_path}")
        meta_resp.raise_for_status()
        meta = meta_resp.json()

        total_size = meta["size"]
        chunks = sorted(meta["chunks"], key=lambda c: c["index"])
        bytes_done = 0

        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)

        with open(local_path, "wb") as f:
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                nodes = chunk["nodes"]
                expected_checksum = chunk.get("checksum")
                data = None

                for node in nodes:
                    try:
                        url = self._chunk_url(node, chunk_id)
                        r = requests.get(url, timeout=30)
                        r.raise_for_status()
                        data = r.content
                        # Verify checksum
                        if expected_checksum and self._checksum(data) != expected_checksum:
                            print(f"  [warn] checksum mismatch on chunk {chunk['index']} from {node['node_id']}, trying next replica")
                            data = None
                            continue
                        break
                    except Exception as exc:
                        print(f"  [warn] could not fetch chunk {chunk['index']} from {node['node_id']}: {exc}")

                if data is None:
                    raise RuntimeError(f"chunk {chunk['index']} unavailable from all replicas")

                f.write(data)
                bytes_done += len(data)
                if progress:
                    progress(bytes_done, total_size)

        return {"path": local_path, "size": total_size, "num_chunks": len(chunks)}

    # ------------------------------------------------------------------ #
    # List / Info / Delete                                                  #
    # ------------------------------------------------------------------ #

    def list_files(self) -> dict:
        r = self._meta("/files", timeout=10)
        r.raise_for_status()
        return r.json()

    def file_info(self, remote_path: str) -> dict:
        r = self._meta("/files/download", params={"path": remote_path}, timeout=10)
        if r.status_code == 404:
            raise FileNotFoundError(f"remote file not found: {remote_path}")
        r.raise_for_status()
        return r.json()

    def delete(self, remote_path: str) -> dict:
        r = requests.delete(f"{self.base_url}/files/delete", params={"path": remote_path}, timeout=10)
        if r.status_code == 404:
            raise FileNotFoundError(f"remote file not found: {remote_path}")
        r.raise_for_status()
        return r.json()

    def list_nodes(self) -> dict:
        r = self._meta("/nodes", timeout=10)
        r.raise_for_status()
        return r.json()

    def health(self) -> dict:
        r = self._meta("/health", timeout=5)
        r.raise_for_status()
        return r.json()

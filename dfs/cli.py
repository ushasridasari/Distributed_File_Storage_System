"""
DFS Command-Line Interface.

Usage examples:
  python -m dfs.cli upload ./photo.jpg /photos/photo.jpg
  python -m dfs.cli download /photos/photo.jpg ./photo_copy.jpg
  python -m dfs.cli ls
  python -m dfs.cli info /photos/photo.jpg
  python -m dfs.cli delete /photos/photo.jpg
  python -m dfs.cli nodes
  python -m dfs.cli health
"""

import sys
import time
import datetime

import click
from colorama import Fore, Style, init as colorama_init
from tabulate import tabulate

from dfs.client import DFSClient
from dfs.config import METADATA_SERVER_HOST, METADATA_SERVER_PORT

colorama_init(autoreset=True)


def _client(host, port) -> DFSClient:
    return DFSClient(metadata_host=host, metadata_port=port)


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _fmt_time(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _progress_bar(label: str):
    """Returns a progress callback that prints a simple bar."""
    start = time.time()

    def _cb(done: int, total: int):
        pct = done / total if total else 1
        filled = int(pct * 40)
        bar = "█" * filled + "░" * (40 - filled)
        elapsed = time.time() - start
        speed = done / elapsed if elapsed > 0 else 0
        print(
            f"\r  {label} [{bar}] {pct*100:.1f}%  {_fmt_bytes(done)}/{_fmt_bytes(total)}"
            f"  {_fmt_bytes(speed)}/s   ",
            end="",
            flush=True,
        )
        if done >= total:
            print()

    return _cb


@click.group()
@click.option("--host", default=METADATA_SERVER_HOST, show_default=True, help="Metadata server host")
@click.option("--port", default=METADATA_SERVER_PORT, show_default=True, type=int, help="Metadata server port")
@click.pass_context
def cli(ctx, host, port):
    """Distributed File Storage System CLI"""
    ctx.ensure_object(dict)
    ctx.obj["host"] = host
    ctx.obj["port"] = port


# --------------------------------------------------------------------------- #
# upload                                                                        #
# --------------------------------------------------------------------------- #

@cli.command()
@click.argument("local_path")
@click.argument("remote_path")
@click.pass_context
def upload(ctx, local_path, remote_path):
    """Upload LOCAL_PATH to REMOTE_PATH on the DFS."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    print(f"{Fore.CYAN}Uploading{Style.RESET_ALL} {local_path} -> {remote_path}")
    try:
        result = client.upload(local_path, remote_path, progress=_progress_bar("Uploading"))
        print(
            f"{Fore.GREEN}✓ Uploaded{Style.RESET_ALL} {result['path']}  "
            f"({_fmt_bytes(result['size'])}, {result['num_chunks']} chunk(s))"
        )
    except FileNotFoundError as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{Fore.RED}Upload failed:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)


# --------------------------------------------------------------------------- #
# download                                                                      #
# --------------------------------------------------------------------------- #

@cli.command()
@click.argument("remote_path")
@click.argument("local_path")
@click.pass_context
def download(ctx, remote_path, local_path):
    """Download REMOTE_PATH from the DFS to LOCAL_PATH."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    print(f"{Fore.CYAN}Downloading{Style.RESET_ALL} {remote_path} -> {local_path}")
    try:
        result = client.download(remote_path, local_path, progress=_progress_bar("Downloading"))
        print(
            f"{Fore.GREEN}✓ Downloaded{Style.RESET_ALL} {result['path']}  "
            f"({_fmt_bytes(result['size'])}, {result['num_chunks']} chunk(s))"
        )
    except FileNotFoundError as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{Fore.RED}Download failed:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)


# --------------------------------------------------------------------------- #
# ls                                                                            #
# --------------------------------------------------------------------------- #

@cli.command(name="ls")
@click.pass_context
def list_files(ctx):
    """List all files stored in the DFS."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    try:
        files = client.list_files()
    except Exception as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)

    if not files:
        print("(no files stored)")
        return

    rows = []
    for path, meta in sorted(files.items()):
        rows.append([
            path,
            _fmt_bytes(meta["size"]),
            meta["num_chunks"],
            _fmt_time(meta["created_at"]),
        ])

    print(tabulate(rows, headers=["Path", "Size", "Chunks", "Created At"], tablefmt="rounded_outline"))


# --------------------------------------------------------------------------- #
# info                                                                          #
# --------------------------------------------------------------------------- #

@cli.command()
@click.argument("remote_path")
@click.pass_context
def info(ctx, remote_path):
    """Show detailed chunk info for a remote file."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    try:
        meta = client.file_info(remote_path)
    except FileNotFoundError as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{Fore.CYAN}File:{Style.RESET_ALL}     {meta['path']}")
    print(f"{Fore.CYAN}Size:{Style.RESET_ALL}     {_fmt_bytes(meta['size'])}")
    print(f"{Fore.CYAN}Chunks:{Style.RESET_ALL}   {meta['num_chunks']}")
    print(f"{Fore.CYAN}Created:{Style.RESET_ALL}  {_fmt_time(meta['created_at'])}\n")

    rows = []
    for chunk in sorted(meta["chunks"], key=lambda c: c["index"]):
        node_list = ", ".join(n["node_id"][:8] + "..." for n in chunk["nodes"])
        rows.append([
            chunk["index"],
            chunk["chunk_id"][:12] + "...",
            _fmt_bytes(chunk.get("size", 0)),
            chunk.get("checksum", "")[:8] + "...",
            node_list,
        ])

    print(tabulate(rows, headers=["#", "Chunk ID", "Size", "MD5", "Nodes"], tablefmt="rounded_outline"))


# --------------------------------------------------------------------------- #
# delete                                                                        #
# --------------------------------------------------------------------------- #

@cli.command()
@click.argument("remote_path")
@click.confirmation_option(prompt="Are you sure you want to delete this file?")
@click.pass_context
def delete(ctx, remote_path):
    """Delete a file from the DFS."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    try:
        result = client.delete(remote_path)
        print(
            f"{Fore.GREEN}✓ Deleted{Style.RESET_ALL} {remote_path}  "
            f"({result['chunks_removed']} chunk(s) removed)"
        )
    except FileNotFoundError as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{Fore.RED}Delete failed:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)


# --------------------------------------------------------------------------- #
# nodes                                                                         #
# --------------------------------------------------------------------------- #

@cli.command()
@click.pass_context
def nodes(ctx):
    """List all registered storage nodes and their status."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    try:
        node_map = client.list_nodes()
    except Exception as e:
        print(f"{Fore.RED}Error:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)

    if not node_map:
        print("(no nodes registered)")
        return

    rows = []
    for nid, info in node_map.items():
        age = info["last_heartbeat"]
        status = f"{Fore.GREEN}live{Style.RESET_ALL}" if age < 10 else f"{Fore.RED}stale{Style.RESET_ALL}"
        rows.append([
            nid[:8] + "...",
            f"{info['host']}:{info['port']}",
            status,
            info["chunks_stored"],
            _fmt_bytes(info["bytes_stored"]),
            f"{age:.1f}s ago",
        ])

    print(tabulate(
        rows,
        headers=["Node ID", "Address", "Status", "Chunks", "Used", "Last Beat"],
        tablefmt="rounded_outline",
    ))


# --------------------------------------------------------------------------- #
# health                                                                        #
# --------------------------------------------------------------------------- #

@cli.command()
@click.pass_context
def health(ctx):
    """Check metadata server health."""
    client = _client(ctx.obj["host"], ctx.obj["port"])
    try:
        h = client.health()
        print(
            f"{Fore.GREEN}✓ Metadata server is up{Style.RESET_ALL}  "
            f"nodes={h['nodes']}  files={h['files']}"
        )
    except Exception as e:
        print(f"{Fore.RED}✗ Metadata server unreachable:{Style.RESET_ALL} {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cli(obj={})

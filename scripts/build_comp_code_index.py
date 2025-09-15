#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_comp_code_index.py — Step-by-step builder for company-ID (comp_code) inverted index.

STEPS
1) Parse arguments & resolve paths
2) Prepare output folders (opt: disable Spotlight indexing)
3) Scan docmeta.bin (mmapped) and bucket RIDs per company id
4) Write shard files (resume-safe, two-level sharding, atomic writes) with live progress
5) (Optional) Write monolithic index
6) Print a final summary

Row layout in docmeta.bin (little-endian int32 x 6):
  [date_int, loc_id, type_id, comp_code, ad_id, ad_link_code]
"""

# ──────────────────────────────────────────────────────────────────────────────
# Step 0: Imports
# ──────────────────────────────────────────────────────────────────────────────
import os
import sys
import json
import time
import mmap
import struct
import argparse
from collections import defaultdict
from typing import Dict, List

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Parse arguments & resolve paths
# ──────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Build inverted index for comp_code.")
    ap.add_argument("--project-root", default=os.environ.get("PROJECT_ROOT", os.getcwd()))
    ap.add_argument("--data-root",    default=os.environ.get("DATA_ROOT"))
    ap.add_argument("--docmeta-bin",  default=os.environ.get("DOCMETA_BIN"))
    ap.add_argument("--shards-root",  default=os.environ.get("SHARDS_ROOT"))
    ap.add_argument("--index-root",   default=os.environ.get("INDEX_ROOT"))
    ap.add_argument("--two-level",    action="store_true",
                    help="Use two-level sharding: comp_code/xx/<id>.json (recommended).")
    ap.add_argument("--no-two-level", dest="two_level", action="store_false",
                    help="Force single-level sharding: comp_code/<id>.json")
    ap.set_defaults(two_level=True)  # default ON

    ap.add_argument("--shards-only",  action="store_true", help="Skip monolithic index write.")
    ap.add_argument("--mono-only",    action="store_true", help="Skip shards; write only monolithic.")

    ap.add_argument("--progress",       type=int, default=250_000,
                    help="Print row-scan progress every N rows (0=off).")
    ap.add_argument("--progress-files", type=int, default=5_000,
                    help="Print shard-write progress every N files (0=off).")
    ap.add_argument("--sample",         type=int, default=0,
                    help="Process only first N rows (benchmark/debug).")
    ap.add_argument("--no-spotlight",   action="store_true",
                    help="Create .metadata_never_index in shard dir to reduce mdworker I/O.")
    return ap.parse_args()

def resolve_paths(args):
    ROOT        = args.project_root
    DATA_ROOT   = args.data_root   or os.path.join(ROOT, "data")
    DOCMETA_BIN = args.docmeta_bin or os.path.join(DATA_ROOT, "docmeta", "docmeta.bin")
    SHARDS_ROOT = args.shards_root or os.path.join(DATA_ROOT, "index_sharded")
    INDEX_ROOT  = args.index_root  or os.path.join(DATA_ROOT, "index")
    return ROOT, DATA_ROOT, DOCMETA_BIN, SHARDS_ROOT, INDEX_ROOT

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Prepare output folders (opt: disable Spotlight indexing)
# ──────────────────────────────────────────────────────────────────────────────
def prepare_outputs(SHARDS_ROOT: str, INDEX_ROOT: str, two_level: bool, no_spotlight: bool):
    comp_dir = os.path.join(SHARDS_ROOT, "comp_code")
    os.makedirs(comp_dir, exist_ok=True)
    os.makedirs(INDEX_ROOT, exist_ok=True)

    if two_level:
        # Pre-create 256 subfolders (00..ff) to avoid mkdir races & speed up writes
        for i in range(256):
            os.makedirs(os.path.join(comp_dir, f"{i:02x}"), exist_ok=True)

    if no_spotlight:
        # Hint Spotlight to ignore this directory on macOS
        try:
            with open(os.path.join(comp_dir, ".metadata_never_index"), "w") as _:
                pass
        except Exception:
            pass

    return comp_dir

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Scan docmeta.bin (mmapped) and bucket RIDs per company id
# ──────────────────────────────────────────────────────────────────────────────
ROW_SIZE = 24  # 6 * int32
UNPACK   = struct.Struct("<6i").unpack_from

def scan_docmeta(DOCMETA_BIN: str, sample: int, progress: int) -> Dict[int, List[int]]:
    t0 = time.perf_counter()
    with open(DOCMETA_BIN, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        total_rows = mm.size() // ROW_SIZE
        limit = min(sample, total_rows) if sample > 0 else total_rows

        buckets: Dict[int, List[int]] = defaultdict(list)
        for rid in range(limit):
            # date_int, loc_id, type_id, comp_code, ad_id, ad_link_code
            _, _, _, comp_code, _, _ = UNPACK(mm, rid * ROW_SIZE)
            buckets[comp_code].append(rid)

            if progress and (rid + 1) % progress == 0:
                elapsed = time.perf_counter() - t0
                rate = (rid + 1) / elapsed if elapsed > 0 else 0
                print(f"[scan] {rid + 1:,}/{limit:,} rows  |  {rate:,.0f} rows/s")

        mm.close()
    print(f"[scan] done: {limit:,} rows → {len(buckets):,} unique companies "
          f"in {time.perf_counter() - t0:,.2f}s")
    return buckets

# ──────────────────────────────────────────────────────────────────────────────
# Step 4: Write shard files (resume-safe, two-level, atomic) with progress
# ──────────────────────────────────────────────────────────────────────────────
def shard_path(comp_dir: str, two_level: bool, comp_code: int) -> str:
    if two_level:
        sub = f"{comp_code & 0xFF:02x}"
        return os.path.join(comp_dir, sub, f"{comp_code}.json")
    return os.path.join(comp_dir, f"{comp_code}.json")

def atomic_write_json(path: str, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as out:
        json.dump(data, out, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)

def write_shards(comp_dir: str, two_level: bool, buckets: Dict[int, List[int]], progress_files: int):
    print("[write] writing shard files …")
    t0 = time.perf_counter()
    items = list(buckets.items())
    total = len(items)
    files_written = 0
    bytes_written = 0

    for i, (comp_code, rids) in enumerate(items, 1):
        rids.sort()
        out_path = shard_path(comp_dir, two_level, comp_code)

        # Resume-safe: skip if already exists
        if os.path.exists(out_path):
            continue

        atomic_write_json(out_path, rids)
        files_written += 1
        try:
            bytes_written += os.path.getsize(out_path)
        except Exception:
            pass

        if progress_files and (i % progress_files == 0 or i == total):
            elapsed = time.perf_counter() - t0
            fps = files_written / elapsed if elapsed > 0 else 0.0
            print(f"[write] {i:,}/{total:,} keys  |  new files: {files_written:,}  "
                  f"|  {fps:,.0f} files/s  |  {bytes_written/1e9:,.2f} GB")

    print(f"[write] shards done in {time.perf_counter() - t0:,.2f}s "
          f"(new files: {files_written:,}, total bytes ~ {bytes_written/1e9:,.2f} GB)")

# ──────────────────────────────────────────────────────────────────────────────
# Step 5: (Optional) Write monolithic index
# ──────────────────────────────────────────────────────────────────────────────
def write_monolithic(INDEX_ROOT: str, buckets: Dict[int, List[int]]):
    print("[mono] writing comp_code.json …")
    t0 = time.perf_counter()
    # Ensure deterministic output: sort lists and keys
    mono = {str(k): sorted(v) for k, v in buckets.items()}
    out_path = os.path.join(INDEX_ROOT, "comp_code.json")
    atomic_write_json(out_path, mono)
    print(f"[mono] done in {time.perf_counter() - t0:,.2f}s  ({len(mono):,} keys)")

# ──────────────────────────────────────────────────────────────────────────────
# Step 6: Orchestrate & summarize
# ──────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    ROOT, DATA_ROOT, DOCMETA_BIN, SHARDS_ROOT, INDEX_ROOT = resolve_paths(args)

    print("=== build_comp_code_index.py ===")
    print(f"ROOT={ROOT}")
    print(f"DATA_ROOT={DATA_ROOT}")
    print(f"DOCMETA_BIN={DOCMETA_BIN}")
    print(f"SHARDS_ROOT={SHARDS_ROOT}")
    print(f"INDEX_ROOT={INDEX_ROOT}")
    print(f"two_level={args.two_level}, shards_only={args.shards_only}, mono_only={args.mono_only}")
    print(f"progress(rows)={args.progress}, progress(files)={args.progress_files}, sample={args.sample}")
    print("================================")

    # Prepare outputs
    comp_dir = prepare_outputs(SHARDS_ROOT, INDEX_ROOT, args.two_level, args.no_spotlight)

    try:
        # Scan & bucket
        buckets = scan_docmeta(DOCMETA_BIN, args.sample, args.progress)

        # Write shards and/or monolithic
        if not args.mono_only:
            write_shards(comp_dir, args.two_level, buckets, args.progress_files)
        if not args.shards_only:
            write_monolithic(INDEX_ROOT, buckets)

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user. Partial results remain on disk (resume-safe).")
        sys.exit(130)

    print("[done] comp_code index build complete.")

if __name__ == "__main__":
    main()
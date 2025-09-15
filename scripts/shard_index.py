#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shard a monolithic index JSON into per-key files.

INPUT:
  index JSON file with shape:
    {
      "<key1>": [id0, id1, ...],  # ids sorted ascending
      "<key2>": [ ... ],
      ...
    }

OUTPUT (to --out):
  - <key>.json for every key (exact postings array, no changes)
  - _meta.json { "src": <path>, "keys": <count>, "postings_total": <sum> }

Use this for HOT, moderate-cardinality indexes:
  - date_int.json   (daily keys; ~24k–30k keys)
  - loc_id.json     (few hundred)
  - type_id.json    (few hundred)

Do NOT shard extremely high-cardinality indexes (e.g. comp_name, ad_id)
unless you use a smarter bucketing scheme—otherwise you’ll create millions
of tiny files.

Requires: ijson (for streaming monolithic JSON).
"""

import argparse, json, os, ijson, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True, help="Path to index JSON (key->[ids])")
    ap.add_argument("--out", required=True, help="Output dir (keys become <key>.json)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    total_keys = 0
    total_ids = 0

    # Stream through the big JSON so memory stays small
    with open(args.index, "r", encoding="utf-8") as f:
        try:
            for key, arr in ijson.kvitems(f, ''):
                p = os.path.join(args.out, f"{key}.json")
                with open(p, "w", encoding="utf-8") as out:
                    json.dump(arr, out, ensure_ascii=False)
                total_keys += 1
                total_ids += len(arr)
                if total_keys % 10_000 == 0:
                    print(f"[info] sharded keys={total_keys:,}", file=sys.stderr)
        except Exception as e:
            print(f"[fatal] while reading {args.index}: {e}", file=sys.stderr)
            raise

    meta = {"src": args.index, "keys": total_keys, "postings_total": total_ids}
    with open(os.path.join(args.out, "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps({"ok": True, "keys": total_keys, "postings_total": total_ids, "out": args.out}))

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a **binary docmeta** file for fast id→record hydration.

INPUT (GZ JSONL):
  Each line is one record with fields (your simplified schema):
    {
      "id": <int, dense 0..N-1>,
      "date_int": <int, seconds since 1960-01-01>,
      "loc_id": <int>,
      "type_id": <int>,
      "comp_name": <int>,      # company-name code
      "ad_id": <int>,          # ilan_id as integer
      "ad_link": "<digits>"    # 7..9 digit *string* (will be stored as int)
    }

OUTPUT (to --out directory):
  - docmeta.bin  : fixed-width binary, struct <6i> per row
                   [date_int, loc_id, type_id, comp_name, ad_id, ad_link_int]
  - meta.json    : {"rows": N, "schema": [...], "struct": "<6i"}

Why:
  The server memory-maps docmeta.bin and can hydrate any id in O(1)
  without loading 22M JSON rows into memory.

Constraints:
  - All values must fit into 32-bit signed int (±2,147,483,647).
    (Your current ranges do.)
  - Line order must correspond to the doc `id` (dense 0..N-1).
"""

import argparse, json, gzip, os, struct, sys

REC = struct.Struct("<6i")  # [date_int, loc_id, type_id, comp_name, ad_id, ad_link_int]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc", required=True, help="Path to ilanlar_simplified_all.jsonl.gz")
    ap.add_argument("--out", required=True, help="Output dir (writes docmeta.bin and meta.json)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    bin_path = os.path.join(args.out, "docmeta.bin")
    meta_path = os.path.join(args.out, "meta.json")

    n = 0
    with gzip.open(args.doc, "rt", encoding="utf-8") as fin, open(bin_path, "wb") as fout:
        for line in fin:
            r = json.loads(line)
            try:
                # Pull and validate integers
                rec = (
                    int(r["date_int"]),
                    int(r["loc_id"]),
                    int(r["type_id"]),
                    int(r["comp_name"]),
                    int(r["ad_id"]),
                    int(r["ad_link"]),     # ad_link stored as int
                )
            except Exception as e:
                print(f"[fatal] row {n}: {e}", file=sys.stderr)
                raise
            # Pack into binary
            fout.write(REC.pack(*rec))
            n += 1
            if n % 1_000_000 == 0:
                print(f"[info] written {n:,} rows…", file=sys.stderr)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "rows": n,
                "schema": ["date_int","loc_id","type_id","comp_name","ad_id","ad_link_int"],
                "struct": "<6i"
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(json.dumps({"ok": True, "rows": n, "bin": bin_path, "meta": meta_path}))

if __name__ == "__main__":
    main()

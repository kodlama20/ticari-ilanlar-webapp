#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build recent company ID set from docmeta + date_int postings and save to:
  LOOKUP_ROOT/recent_company_ids_y{N}.json

Usage (from project root):
  # EITHER run directly (self-resolving sys.path)
  RECENT_YEARS=10 python scripts/build_recent_companies.py

  # OR as a module (if you add scripts/__init__.py):
  RECENT_YEARS=10 python -m scripts.build_recent_companies
"""

from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timezone, timedelta

# ---- Make project root importable when running as a script ----
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---- Project imports (now work regardless of how invoked) ----
from server.config import LOOKUP_ROOT, DOCMETA_BIN
from server.docmeta import DocMeta
from server.indexes import postings

# ---- Date helpers (match server) ----
EPOCH_1960 = datetime(1960, 1, 1, tzinfo=timezone.utc)
DAY_SECS = 86400

def iso_to_sec1960(iso: str) -> int:
    y, m, d = [int(x) for x in iso.split("-")]
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    return int((dt - EPOCH_1960).total_seconds())

def date_keys_for_range(a_iso: str, b_iso: str):
    a = iso_to_sec1960(a_iso); b = iso_to_sec1960(b_iso)
    if a > b: a, b = b, a
    x = a - (a % DAY_SECS)
    y = b - (b % DAY_SECS)
    out = []
    while x <= y:
        out.append(x)
        x += DAY_SECS
    return out

def main():
    yrs = int(os.getenv("RECENT_YEARS", "10"))
    out_path = os.path.join(LOOKUP_ROOT, f"recent_company_ids_y{yrs}.json")

    dm = DocMeta(DOCMETA_BIN)
    try:
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=365 * yrs)
        keys = date_keys_for_range(start.isoformat(), today.isoformat())

        ids = set()
        for k in keys:
            arr = postings("date_int", k)
            if not arr:
                continue
            for rid in arr:
                _, _, _, comp_code, _, _ = dm.get_row(rid)
                ids.add(int(comp_code))

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f, ensure_ascii=False)
        print(f"wrote {len(ids)} recent IDs -> {out_path}")
    finally:
        dm.close()

if __name__ == "__main__":
    main()

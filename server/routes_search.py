#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
routes_search.py
================

Implements the /search endpoint.

Key points:
- Uses inverted indexes (JSON or shards) for loc_id, type_id, date_int.
- Optionally unions per-day postings for date ranges if that looks selective.
- Intersects the smallest postings first.
- Hydrates the final rows with vocab lookups.

IMPORTANT: We call `ensure_docmeta_loaded()` before touching doc rows to
guarantee the mmap is open (lazy-load).
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .config import INDEX_ROOT, SHARDS_ROOT
from .docmeta import ensure_docmeta_loaded
from .lookups import MUDURLUK_NAMES, ILAN_TURU_NAMES, UNVAN_VOCAB

# ──────────────────────────────────────────────────────────────────────────────
# Pydantic request models (keep identical to your existing schemas)
# ──────────────────────────────────────────────────────────────────────────────
class SearchFilters(BaseModel):
    date_from: Optional[str] = None
    date_to:   Optional[str] = None
    company_code: Optional[int] = None
    city_code:    Optional[int] = None
    type_code:    Optional[int] = None
    limit:        Optional[int] = None

class SearchIn(BaseModel):
    filters: SearchFilters
    limit: Optional[int] = 40


router = APIRouter()


# ──────────────────────────────────────────────────────────────────────────────
# Simple set ops (sorted lists of ints)
# ──────────────────────────────────────────────────────────────────────────────
def intersect_sorted(a: List[int], b: List[int]) -> List[int]:
    i = j = 0
    out: List[int] = []
    na, nb = len(a), len(b)
    while i < na and j < nb:
        va, vb = a[i], b[j]
        if va == vb:
            out.append(va); i += 1; j += 1
        elif va < vb:
            i += 1
        else:
            j += 1
    return out

def union_sorted(a: List[int], b: List[int]) -> List[int]:
    i = j = 0
    out: List[int] = []
    na, nb = len(a), len(b)
    last = None
    while i < na and j < nb:
        va, vb = a[i], b[j]
        if va == vb:
            if last != va: out.append(va); last = va
            i += 1; j += 1
        elif va < vb:
            if last != va: out.append(va); last = va
            i += 1
        else:
            if last != vb: out.append(vb); last = vb
            j += 1
    while i < na:
        va = a[i]
        if last != va: out.append(va); last = va
        i += 1
    while j < nb:
        vb = b[j]
        if last != vb: out.append(vb); last = vb
        j += 1
    return out

def union_many(sorted_lists: List[List[int]]) -> List[int]:
    if not sorted_lists:
        return []
    cur = sorted_lists[0]
    for k in range(1, len(sorted_lists)):
        if not cur:
            break
        cur = union_sorted(cur, sorted_lists[k])
    return cur


# ──────────────────────────────────────────────────────────────────────────────
# Date helpers (keys are seconds since 1960-01-01, day-aligned)
# ──────────────────────────────────────────────────────────────────────────────
_DAY_SECS = 86400

def _iso_to_sec1960(iso: str) -> int:
    y, m, d = [int(x) for x in iso.split("-")]
    # Epoch 1960-01-01 UTC
    # We do the math inline to keep this file self-contained.
    from datetime import datetime, timezone
    epoch = datetime(1960, 1, 1, tzinfo=timezone.utc)
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    return int((dt - epoch).total_seconds())

def _date_keys_for_range(date_from_iso: str, date_to_iso: str) -> List[int]:
    a = _iso_to_sec1960(date_from_iso)
    b = _iso_to_sec1960(date_to_iso)
    if a > b: a, b = b, a
    keys = []
    x = a - (a % _DAY_SECS)
    y = b - (b % _DAY_SECS)
    while x <= y:
        keys.append(x)
        x += _DAY_SECS
    return keys


# ──────────────────────────────────────────────────────────────────────────────
# Inverted index readers (monolithic JSON or per-key shards)
# ──────────────────────────────────────────────────────────────────────────────
def _shard_path(index: str, key: int) -> str:
    return os.path.join(SHARDS_ROOT, index, f"{key}.json")

def postings_from_shard(index: str, key: int) -> Optional[List[int]]:
    path = _shard_path(index, key)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

@lru_cache(maxsize=8)
def load_monolithic_index(index: str) -> Dict[str, List[int]]:
    path = os.path.join(INDEX_ROOT, f"{index}.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def postings(index: str, key: int) -> List[int]:
    arr = postings_from_shard(index, key)
    if arr is not None:
        return arr
    mono = load_monolithic_index(index)
    return mono.get(str(key), [])


# ──────────────────────────────────────────────────────────────────────────────
# Row hydration (uses lazy-loaded DOCMETA)
# ──────────────────────────────────────────────────────────────────────────────
def hydrate_row(rid: int) -> dict:
    DOCMETA = ensure_docmeta_loaded()
    di, loc, typ, comp_code, adid, _ = DOCMETA.get_row(rid)
    return {
        "id": rid,
        "date_int": di,
        "loc_id": loc,
        "type_id": typ,
        "comp_name": comp_code,
        "ad_id": adid,
        "company": UNVAN_VOCAB.get(comp_code, ""),
        "city": MUDURLUK_NAMES.get(loc, str(loc)),
        "type": ILAN_TURU_NAMES.get(typ, str(typ)),
        "ad_link": "",  # fill later if you add URL mapping
    }


# ──────────────────────────────────────────────────────────────────────────────
# /search endpoint
# ──────────────────────────────────────────────────────────────────────────────
@router.post("/search")
def search(inp: SearchIn):
    """
    Strategy:
      1) Build postings for city and/or type.
      2) If date range present, decide whether to union per-day postings and include
         as another list to intersect; otherwise we post-filter by date.
      3) Intersect lists from smallest to largest.
      4) Post-filter by date/company if needed, hydrate, cap by limit.
    """
    DOCMETA = ensure_docmeta_loaded()  # lazy open (or HTTP 503)
    f = inp.filters or SearchFilters()
    limit = max(1, min(200, inp.limit or f.limit or 40))

    lists: List[Tuple[str, List[int]]] = []
    city_list: Optional[List[int]] = None
    type_list: Optional[List[int]] = None
    date_list: Optional[List[int]] = None

    # City postings
    if f.city_code is not None:
        city_list = postings("loc_id", int(f.city_code))
        lists.append(("city", city_list))

    # Type postings
    if f.type_code is not None:
        type_list = postings("type_id", int(f.type_code))
        lists.append(("type", type_list))

    # Date range → union of day postings if selective
    have_date = bool(f.date_from and f.date_to)
    if have_date:
        try:
            date_keys = _date_keys_for_range(f.date_from, f.date_to)
            day_lists = [postings("date_int", k) for k in date_keys]
            est = sum(len(x) for x in day_lists)
            bases = []
            if city_list is not None: bases.append(len(city_list))
            if type_list is not None: bases.append(len(type_list))
            min_base = min(bases) if bases else est
            # heuristic multiplier
            if est <= (min_base * 4):
                date_list = union_many(day_lists)
                lists.append(("date", date_list))
        except Exception:
            date_list = None

    if not lists:
        # allow pure date search if we built a date_list
        if date_list is not None:
            lists.append(("date", date_list))
        else:
            raise HTTPException(status_code=400,
                                detail="Provide at least one of: city_code, type_code, or a valid date range.")

    # Intersect smallest → largest
    lists.sort(key=lambda t: len(t[1]))
    cur = lists[0][1]
    for _, arr in lists[1:]:
        if not cur or not arr:
            cur = []
            break
        cur = intersect_sorted(cur, arr)
        if not cur:
            break

    need_date_post = have_date and (date_list is None)
    if need_date_post:
        di_from = _iso_to_sec1960(f.date_from)
        di_to = _iso_to_sec1960(f.date_to)

    hits: List[dict] = []
    for rid in cur:
        di, loc, typ, comp_code, adid, _ = DOCMETA.get_row(rid)
        if need_date_post and (di < di_from or di > di_to):
            continue
        if f.company_code is not None and comp_code != int(f.company_code):
            continue

        hits.append({
            "id": rid,
            "date_int": di,
            "loc_id": loc,
            "type_id": typ,
            "comp_name": comp_code,
            "ad_id": adid,
            "company": UNVAN_VOCAB.get(comp_code, ""),
            "city": MUDURLUK_NAMES.get(loc, str(loc)),
            "type": ILAN_TURU_NAMES.get(typ, str(typ)),
            "ad_link": "",
        })
        if len(hits) >= limit:
            break

    return {"hits": hits, "count": len(hits)}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Load inverted index postings (either monolithic JSON or per-key shard).
All postings are sorted lists of doc row IDs.
"""

import json
import os
from functools import lru_cache
from typing import Dict, List, Optional
from .config import INDEX_ROOT, SHARDS_ROOT

def _from_shard(index: str, key: int) -> Optional[List[int]]:
    p = os.path.join(SHARDS_ROOT, index, f"{key}.json")
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

@lru_cache(maxsize=8)
def _monolithic(index: str) -> Dict[str, List[int]]:
    p = os.path.join(INDEX_ROOT, f"{index}.json")
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def postings(index: str, key: int) -> List[int]:
    arr = _from_shard(index, key)
    if arr is not None:
        return arr
    return _monolithic(index).get(str(key), [])

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
    last = None
    na, nb = len(a), len(b)
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

def union_many(lists: List[List[int]]) -> List[int]:
    if not lists:
        return []
    cur = lists[0]
    for k in range(1, len(lists)):
        if not cur: break
        cur = union_sorted(cur, lists[k])
    return cur

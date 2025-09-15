#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Inverted index loading:
- Prefer sharded JSON (data/index_sharded/<index>/<key>.json) for single-key reads
- Fallback to monolithic JSON (data/index/<index>.json)
- Keys in monolithic JSON are strings
"""

import json
import os
from functools import lru_cache
from typing import Dict, List, Optional

from .config import INDEX_ROOT, SHARDS_ROOT

def postings_from_shard(index: str, key: int) -> Optional[List[int]]:
    shard_file = os.path.join(SHARDS_ROOT, index, f"{key}.json")
    if os.path.exists(shard_file):
        with open(shard_file, "r", encoding="utf-8") as f:
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

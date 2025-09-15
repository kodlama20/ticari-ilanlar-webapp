# server/recency.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Recent companies helper.

- Loads a set of company codes considered "recent" (e.g., appeared in last N years).
- Source file can be specified via env RECENT_COMPANIES_FILE, or defaults to
  LOOKUP_ROOT/recent_company_ids.json.
- Accepts either:
    1) {"ids": [123, 456, "789", ...]}
    2) [123, 456, "789", ...]
"""

from __future__ import annotations
import json
import os
from typing import Iterable, Optional, Set

# This set is populated on startup by `load_recent_companies(...)`
RECENT_COMPANY_IDS: Set[int] = set()

def _coerce_ints(items: Iterable) -> Set[int]:
    out: Set[int] = set()
    for v in items or []:
        try:
            out.add(int(v))
        except Exception:
            # ignore junk
            continue
    return out

def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _default_lookup_root(explicit: Optional[str]) -> str:
    if explicit and explicit.strip():
        return os.path.abspath(explicit)
    env = os.environ.get("LOOKUP_ROOT")
    if env and env.strip():
        return os.path.abspath(env)
    return os.path.abspath(os.path.join(os.getcwd(), "lookup"))

def _resolve_recent_path(lookup_root: Optional[str]) -> str:
    # Highest priority: explicit env variable
    p = os.environ.get("RECENT_COMPANIES_FILE")
    if p and p.strip():
        return os.path.abspath(p)
    # Else: LOOKUP_ROOT/recent_company_ids.json
    base = _default_lookup_root(lookup_root)
    return os.path.join(base, "recent_company_ids.json")

def load_recent_companies(lookup_root: Optional[str] = None) -> int:
    """
    Populate RECENT_COMPANY_IDS from JSON file.
    Returns the number of IDs loaded. If the file is missing or invalid, leaves an empty set.
    """
    global RECENT_COMPANY_IDS
    path = _resolve_recent_path(lookup_root)
    try:
        if not os.path.exists(path):
            RECENT_COMPANY_IDS = set()
            return 0

        data = _read_json(path)
        # Accept either {"ids":[...]} or a plain list [...]
        if isinstance(data, dict) and "ids" in data and isinstance(data["ids"], list):
            RECENT_COMPANY_IDS = _coerce_ints(data["ids"])
        elif isinstance(data, list):
            RECENT_COMPANY_IDS = _coerce_ints(data)
        else:
            # Unrecognized shape → treat as empty
            RECENT_COMPANY_IDS = set()
        return len(RECENT_COMPANY_IDS)
    except Exception:
        # On any error, don't crash startup; just expose 0 and allow health to report it
        RECENT_COMPANY_IDS = set()
        return 0

def is_recent_company(code: int) -> bool:
    """Fast membership check used by tools/lookup_company for biasing suggestions."""
    try:
        return int(code) in RECENT_COMPANY_IDS
    except Exception:
        return False

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lookup tables:
- mudurluk (city/registry)         -> code/name
- ilan_turu (announcement type)    -> code/name
- company vocab (unvan)            -> id/name and optional norm->id map

Notes
-----
- This module normalizes input keys using `norm_tr` so JSON keys like "İZMİR"
  correctly become "izmir" for lookups.
- `load_lookups()` may be called with an explicit path OR with no arguments,
  in which case it uses:
      LOOKUP_ROOT env var  -> if set
      ./lookup             -> fallback

Extra (optional) datasets
-------------------------
- recent_company_ids_y*.json : a JSON list of company IDs active in the last N years.
  We load the file with the largest N (if multiple exist) into UNVAN_RECENT_IDS.
- UNVAN_SAMPLES              : a small list [{"code","name"}] used by tools for quick
  fallback suggestions. Built from recent IDs if available, otherwise from the start
  of UNVAN_VOCAB.
"""

from __future__ import annotations

import json
import os
import glob
from typing import Dict, Set, List

from .normalize import norm_tr

# Module-level singletons (do NOT rebind these names inside load_lookups)
MUDURLUK_CODES: Dict[str, int] = {}   # normalized name -> code (e.g., "izmir" -> 1)
MUDURLUK_NAMES: Dict[int, str] = {}   # code -> canonical name (e.g., 1 -> "İZMİR")
ILAN_TURU_CODES: Dict[str, int] = {}  # normalized type name -> code
ILAN_TURU_NAMES: Dict[int, str] = {}  # code -> canonical type name
UNVAN_VOCAB: Dict[int, str] = {}      # company code -> display name
UNVAN_NAME_TO_ID: Dict[str, int] = {} # normalized name -> company code (optional)

# Optional accelerators
UNVAN_RECENT_IDS: Set[int] = set()    # company IDs active in last N years (if file exists)
UNVAN_SAMPLES: List[Dict[str, object]] = []  # [{"code": int, "name": str}, ...]

__all__ = [
    "MUDURLUK_CODES", "MUDURLUK_NAMES",
    "ILAN_TURU_CODES", "ILAN_TURU_NAMES",
    "UNVAN_VOCAB", "UNVAN_NAME_TO_ID",
    "UNVAN_RECENT_IDS", "UNVAN_SAMPLES",
    "load_lookups",
]


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def _auto_lookup_root(arg_root: str | None) -> str:
    """
    Resolve lookup root:
      1) explicit argument if provided
      2) LOOKUP_ROOT environment variable
      3) ./lookup (project local)
    """
    if arg_root and arg_root.strip():
        return os.path.abspath(arg_root)
    env = os.environ.get("LOOKUP_ROOT")
    if env and env.strip():
        return os.path.abspath(env)
    return os.path.abspath(os.path.join(os.getcwd(), "lookup"))


def _find_recent_ids_file(root: str) -> str | None:
    """
    Find the 'recent company IDs' file with the largest year window, e.g.:
      recent_company_ids_y10.json  (preferred over y5 if both exist)
    Returns absolute path or None.
    """
    candidates = glob.glob(os.path.join(root, "recent_company_ids_y*.json"))
    if not candidates:
        return None

    def years_key(p: str) -> int:
        base = os.path.basename(p)
        try:
            return int(base.split("y")[-1].split(".")[0])
        except Exception:
            return 0

    candidates.sort(key=years_key, reverse=True)
    return os.path.abspath(candidates[0])


def _build_samples_from_dict(d: Dict[int, str], ids: List[int], max_n: int = 200) -> List[Dict[str, object]]:
    """Build a small list of {'code','name'} using provided ids and the vocab dict."""
    out: List[Dict[str, object]] = []
    for cid in ids:
        name = d.get(int(cid))
        if not name:
            continue
        out.append({"code": int(cid), "name": str(name)})
        if len(out) >= max_n:
            break
    return out


def load_lookups(lookup_root: str | None = None):
    """
    Populate the module-level dicts **in place** (no rebinding),
    so any module that already imported these dicts sees the updated contents.

    Parameters
    ----------
    lookup_root : str | None
        Folder containing:
          - mudurluk_codes.json
          - ilan_turu_codes.json
          - unvan_vocab.json
          - unvan_name_to_id.json
          - recent_company_ids_y*.json  (optional)
    """
    root = _auto_lookup_root(lookup_root)

    # ── mudurluk (city/registry) JSON: NAME -> CODE
    mud = _load_json(os.path.join(root, "mudurluk_codes.json"), {})
    MUDURLUK_CODES.clear()
    MUDURLUK_NAMES.clear()
    for name, code in mud.items():
        try:
            canon_name = str(name)
            icode = int(code)
        except Exception:
            continue
        MUDURLUK_CODES[norm_tr(canon_name)] = icode
        if icode not in MUDURLUK_NAMES:
            MUDURLUK_NAMES[icode] = canon_name

    # ── ilan_turu (type) JSON: NAME -> CODE
    typ = _load_json(os.path.join(root, "ilan_turu_codes.json"), {})
    ILAN_TURU_CODES.clear()
    ILAN_TURU_NAMES.clear()
    for name, code in typ.items():
        try:
            canon_name = str(name)
            icode = int(code)
        except Exception:
            continue
        ILAN_TURU_CODES[norm_tr(canon_name)] = icode
        if icode not in ILAN_TURU_NAMES:
            ILAN_TURU_NAMES[icode] = canon_name

    # ── unvan vocab JSON: ID -> NAME
    vocab = _load_json(os.path.join(root, "unvan_vocab.json"), {})
    UNVAN_VOCAB.clear()
    for sid, name in vocab.items():
        try:
            cid = int(sid)
            UNVAN_VOCAB[cid] = str(name)
        except Exception:
            continue

    # ── exact-name map JSON: NAME(norm or not) -> ID
    raw_map = _load_json(os.path.join(root, "unvan_name_to_id.json"), {})
    UNVAN_NAME_TO_ID.clear()
    if isinstance(raw_map, dict):
        for k, v in raw_map.items():
            try:
                cid = int(v)
                key_norm = norm_tr(str(k))
                if key_norm and key_norm not in UNVAN_NAME_TO_ID:
                    UNVAN_NAME_TO_ID[key_norm] = cid
            except Exception:
                continue

    # ── optional recent-company ID set
    UNVAN_RECENT_IDS.clear()
    recent_file = _find_recent_ids_file(root)
    if recent_file and os.path.exists(recent_file):
        try:
            ids = _load_json(recent_file, [])
            UNVAN_RECENT_IDS.update(int(x) for x in ids if isinstance(x, (int, str)))
        except Exception:
            UNVAN_RECENT_IDS.clear()

    # ── build UNVAN_SAMPLES (small list for quick suggestions)
    UNVAN_SAMPLES.clear()
    if UNVAN_RECENT_IDS:
        recent_sorted = sorted(UNVAN_RECENT_IDS)
        UNVAN_SAMPLES.extend(_build_samples_from_dict(UNVAN_VOCAB, recent_sorted, max_n=200))
    if not UNVAN_SAMPLES:
        vocab_ids = sorted(UNVAN_VOCAB.keys())
        UNVAN_SAMPLES.extend(_build_samples_from_dict(UNVAN_VOCAB, vocab_ids, max_n=200))

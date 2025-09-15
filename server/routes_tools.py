#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tool endpoints (public, read-only).

Exposes:
- POST /tools/parse_date_range
- POST /tools/lookup_mudurluk
- POST /tools/lookup_ilan_turu
- POST /tools/lookup_company   <-- exact via name->id; suggestions: recent-first, then bounded global scan

IMPORTANT:
Do NOT import lookup dicts by value (e.g., `from .lookups import UNVAN_VOCAB`).
Always import the MODULE and read `lookups.UNVAN_VOCAB` etc., otherwise the
references will be to the old, empty dicts created at import time.
"""

from __future__ import annotations

from typing import List, Dict
from fastapi import APIRouter
from pydantic import BaseModel

from .normalize import norm_tr
from .dates import parse_date_range_text

# ✅ Import the modules (live views of dicts), not the dicts themselves.
from . import lookups as L            # L.MUDURLUK_CODES, L.UNVAN_VOCAB, ...
from . import recency as R            # R.RECENT_COMPANY_IDS

router = APIRouter()

# -----------------------------
# Request models (pydantic)
# -----------------------------
class ToolDateIn(BaseModel):
    text: str

class ToolMudurlukIn(BaseModel):
    name: str

class ToolIlanTuruIn(BaseModel):
    term: str

class ToolCompanyIn(BaseModel):
    name: str


# -----------------------------
# Config knobs for suggestions
# -----------------------------
# Keep latency bounded. We search recent names first, then a capped global scan.
MATCH_LIMIT = 8               # return up to this many suggestions
RECENT_SCAN_CAP = 120_000     # check up to this many recent-company names
GLOBAL_SCAN_CAP = 200_000     # check up to this many global names
MIN_QUERY_LEN_FOR_FUZZY = 3   # avoid fuzzy matching for very short queries


# -----------------------------
# Helpers
# -----------------------------
def _all_tokens_in_text(tokens: List[str], text_norm: str) -> bool:
    """True if all tokens are present in normalized text."""
    return all(tok in text_norm for tok in tokens)

def _rank_company_options(options: List[Dict]) -> List[Dict]:
    """Rank: recent first, then alphabetical by name."""
    def key(o):
        cid = int(o.get("code", -1))
        recent_flag = 0 if cid in R.RECENT_COMPANY_IDS else 1
        return (recent_flag, o.get("name", ""))
    return sorted(options, key=key)


# -----------------------------
# /tools/parse_date_range
# -----------------------------
@router.post("/tools/parse_date_range")
def tool_parse_date_range(inp: ToolDateIn):
    rng = parse_date_range_text(inp.text or "")
    if not rng:
        return {"status": "unmapped"}
    a, b = rng
    return {"status": "ok", "range": {"from": a, "to": b}}


# -----------------------------
# /tools/lookup_mudurluk
# -----------------------------
@router.post("/tools/lookup_mudurluk")
def tool_lookup_mudurluk(inp: ToolMudurlukIn):
    q = norm_tr(inp.name or "")
    if not q:
        return {"status": "unmapped"}

    code = L.MUDURLUK_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": code, "name": L.MUDURLUK_NAMES.get(code, inp.name)}

    # small suggestion set (prefix/contains)
    opts = []
    for canon, c in L.MUDURLUK_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": c, "name": L.MUDURLUK_NAMES.get(c, canon)})
            if len(opts) >= 6:
                break

    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}


# -----------------------------
# /tools/lookup_ilan_turu
# -----------------------------
@router.post("/tools/lookup_ilan_turu")
def tool_lookup_ilan_turu(inp: ToolIlanTuruIn):
    q = norm_tr(inp.term or "")
    if not q:
        return {"status": "unmapped"}

    code = L.ILAN_TURU_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": code, "name": L.ILAN_TURU_NAMES.get(code, inp.term)}

    opts = []
    for canon, c in L.ILAN_TURU_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": c, "name": L.ILAN_TURU_NAMES.get(c, canon)})
            if len(opts) >= 6:
                break

    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}


# -----------------------------
# /tools/lookup_company
# -----------------------------
@router.post("/tools/lookup_company")
def tool_lookup_company(inp: ToolCompanyIn):
    """
    Resolve company by name:

    1) EXACT: normalized name -> id via L.UNVAN_NAME_TO_ID (O(1)).
    2) SUGGEST (recent-first): scan up to RECENT_SCAN_CAP ids from L.UNVAN_VOCAB
       where id ∈ R.RECENT_COMPANY_IDS, match by all tokens (substring).
    3) SUGGEST (global bounded): if still not enough, scan up to GLOBAL_SCAN_CAP
       names from L.UNVAN_VOCAB globally for substring tokens.

    NOTE: We read dicts via the module `L`, so we always see the dictionaries
    that `load_lookups()` populated at startup.
    """
    try:
        q_raw = (inp.name or "").strip()
    except Exception:
        q_raw = ""
    q = norm_tr(q_raw)

    if not q:
        return {"status": "unmapped"}

    # (1) exact via massive normalized map (fast path)
    code = L.UNVAN_NAME_TO_ID.get(q)
    if code is not None:
        try:
            code_int = int(code)
        except Exception:
            code_int = code
        return {
            "status": "ok",
            "code": int(code_int),
            "name": L.UNVAN_VOCAB.get(int(code_int), q_raw),
        }

    # Very short queries -> don't fuzzy match (avoid noise)
    if len(q) < MIN_QUERY_LEN_FOR_FUZZY:
        return {"status": "unmapped"}

    tokens = [t for t in q.split() if t]
    if not tokens:
        return {"status": "unmapped"}

    # (2) recent-first fuzzy pass
    opts: List[Dict] = []
    checked = 0
    # Iterate deterministically; check only recent ids
    for cid, name in L.UNVAN_VOCAB.items():
        if checked >= RECENT_SCAN_CAP or len(opts) >= MATCH_LIMIT:
            break
        if cid not in R.RECENT_COMPANY_IDS:
            continue
        checked += 1
        nn = norm_tr(name)
        if _all_tokens_in_text(tokens, nn):
            opts.append({"code": int(cid), "name": name})

    # (3) global bounded fuzzy pass
    if len(opts) < MATCH_LIMIT:
        checked = 0
        for cid, name in L.UNVAN_VOCAB.items():
            if checked >= GLOBAL_SCAN_CAP or len(opts) >= MATCH_LIMIT:
                break
            checked += 1
            nn = norm_tr(name)
            if _all_tokens_in_text(tokens, nn):
                if not any(o["code"] == int(cid) for o in opts):
                    opts.append({"code": int(cid), "name": name})

    if opts:
        return {"status": "ambiguous", "options": _rank_company_options(opts)[:MATCH_LIMIT]}

    return {"status": "unmapped"}

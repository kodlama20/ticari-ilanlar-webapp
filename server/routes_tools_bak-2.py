#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tool endpoints (public, read-only).

Exposes:
- POST /tools/parse_date_range
- POST /tools/lookup_mudurluk
- POST /tools/lookup_ilan_turu
- POST /tools/lookup_company   <-- exact via name->id; suggestions: recent-first, then bounded global scan
"""

from __future__ import annotations

from typing import List, Dict, Optional, Tuple
from fastapi import APIRouter
from pydantic import BaseModel

# Helper modules from our server package
from .normalize import norm_tr
from .dates import parse_date_range_text
from .lookups import (
    MUDURLUK_CODES, MUDURLUK_NAMES,
    ILAN_TURU_CODES, ILAN_TURU_NAMES,
    UNVAN_VOCAB, UNVAN_NAME_TO_ID,
)
from .recency import RECENT_COMPANY_IDS  # set[int] of recent company ids

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
# When providing suggestions for company names, we keep latency bounded by scanning
# a capped number of entries. We first scan "recent" companies (last N years) and if
# that doesn't yield enough, we broaden to a global bounded scan.
MATCH_LIMIT = 8              # return up to this many suggestions
RECENT_SCAN_CAP = 120_000    # max recent items to check per request
GLOBAL_SCAN_CAP = 200_000    # max global items to check per request
MIN_QUERY_LEN_FOR_FUZZY = 3  # avoid super-short fuzzy matches like "a", "im"


# -----------------------------
# Utility: match + ranking
# -----------------------------
def _all_tokens_in_text(tokens: List[str], text_norm: str) -> bool:
    """Return True if all tokens are contained in text_norm."""
    return all(tok in text_norm for tok in tokens)

def _rank_company_options(options: List[Dict]) -> List[Dict]:
    """
    Rank suggestions: (recent first) then alphabetical by name.
    Each option is {"code": int, "name": str}.
    """
    def key(o):
        cid = int(o.get("code", -1))
        recent_flag = 0 if cid in RECENT_COMPANY_IDS else 1
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

    code = MUDURLUK_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": code, "name": MUDURLUK_NAMES.get(code, inp.name)}

    # Small suggestion list (prefix or contains)
    opts = []
    for canon, c in MUDURLUK_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": c, "name": MUDURLUK_NAMES.get(c, canon)})
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

    code = ILAN_TURU_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": code, "name": ILAN_TURU_NAMES.get(code, inp.term)}

    opts = []
    for canon, c in ILAN_TURU_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": c, "name": ILAN_TURU_NAMES.get(c, canon)})
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
    Resolve company by name with this strategy:

    1) EXACT: normalized name -> id via UNVAN_NAME_TO_ID (O(1)).
    2) SUGGEST (recent-first): scan up to RECENT_SCAN_CAP items from UNVAN_VOCAB
       where company_id ∈ RECENT_COMPANY_IDS, match by substring tokens.
    3) SUGGEST (global bounded): if still not enough, scan up to GLOBAL_SCAN_CAP
       items from UNVAN_VOCAB globally for substring tokens.
    """
    try:
        q_raw = (inp.name or "").strip()
    except Exception:
        q_raw = ""
    q = norm_tr(q_raw)

    if not q:
        return {"status": "unmapped"}

    # EXACT hit via massive normalized map (fast)
    code = UNVAN_NAME_TO_ID.get(q)
    if code is not None:
        try:
            code_int = int(code)
        except Exception:
            code_int = code
        return {
            "status": "ok",
            "code": int(code_int),
            "name": UNVAN_VOCAB.get(int(code_int), q_raw)
        }

    # For very short queries, avoid heavy fuzzy scans
    if len(q) < MIN_QUERY_LEN_FOR_FUZZY:
        return {"status": "unmapped"}

    tokens = [t for t in q.split() if t]
    if not tokens:
        return {"status": "unmapped"}

    # ---- PASS 1: recent-first fuzzy match (bounded)
    opts: List[Dict] = []
    checked = 0
    # Iterate deterministically over UNVAN_VOCAB (dict iteration order is stable per run)
    for cid, name in UNVAN_VOCAB.items():
        if checked >= RECENT_SCAN_CAP or len(opts) >= MATCH_LIMIT:
            break
        if cid not in RECENT_COMPANY_IDS:
            continue
        checked += 1
        nn = norm_tr(name)
        if _all_tokens_in_text(tokens, nn):
            opts.append({"code": int(cid), "name": name})

    # ---- PASS 2: broaden to global bounded fuzzy scan
    if len(opts) < MATCH_LIMIT:
        checked = 0
        for cid, name in UNVAN_VOCAB.items():
            if checked >= GLOBAL_SCAN_CAP or len(opts) >= MATCH_LIMIT:
                break
            checked += 1
            nn = norm_tr(name)
            if _all_tokens_in_text(tokens, nn):
                # Avoid duplicates if it was already added from recent pass
                if not any(o["code"] == int(cid) for o in opts):
                    opts.append({"code": int(cid), "name": name})

    if opts:
        return {"status": "ambiguous", "options": _rank_company_options(opts)[:MATCH_LIMIT]}

    return {"status": "unmapped"}

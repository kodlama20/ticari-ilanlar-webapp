# server/routes_tools.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
/tools/* endpoints (date parsing + lookups)

- /tools/parse_date_range
- /tools/lookup_mudurluk
- /tools/lookup_ilan_turu
- /tools/lookup_company   (with recent-company bias in suggestions)

This module reads in-memory lookup tables populated at startup by
server.lookups.load_lookups().  We avoid heavy CPU work here because
endpoints are called frequently by the UI.
"""

from __future__ import annotations
from typing import List, Optional, Dict

from fastapi import APIRouter
from pydantic import BaseModel

from .dateparse import parse_date_range_text
from .normalize import norm_tr
from . import lookups as L  # module holds the dicts loaded at startup
from .recency import RECENT_COMPANY_IDS, is_recent_company  # <-- correct imports

router = APIRouter()

# ---------- Pydantic DTOs ----------

class ToolDateIn(BaseModel):
    text: str

class ToolMudurlukIn(BaseModel):
    name: str

class ToolIlanTuruIn(BaseModel):
    term: str

class ToolCompanyIn(BaseModel):
    name: str

# ---------- /tools/parse_date_range ----------

@router.post("/tools/parse_date_range")
def tool_parse_date_range(inp: ToolDateIn):
    """
    Map Turkish/ISO phrases to an inclusive date range [from, to] as YYYY-MM-DD.
    Examples supported (in dateparse.py):
      - 'son 30 gün'
      - '2024-01..2024-03'
      - 'Ocak 2025'
      - '2019'
      - 'son 25 yıl'
    """
    rng = parse_date_range_text(inp.text or "")
    if not rng:
        return {"status": "unmapped"}
    return {"status": "ok", "range": {"from": rng[0], "to": rng[1]}}

# ---------- /tools/lookup_mudurluk (city/registry) ----------

@router.post("/tools/lookup_mudurluk")
def tool_lookup_mudurluk(inp: ToolMudurlukIn):
    """
    Resolve a city/registry name to its code. Uses normalized keys.
    Input JSON shape: {"name": "Denizli"}  -> {"status":"ok","code":17,"name":"DENİZLİ"}
    """
    q = norm_tr(inp.name or "")
    if not q:
        return {"status": "unmapped"}
    code = L.MUDURLUK_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": int(code), "name": L.MUDURLUK_NAMES.get(int(code), inp.name)}

    # Suggest a few close matches
    opts = []
    for canon, c in L.MUDURLUK_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": int(c), "name": L.MUDURLUK_NAMES.get(int(c), canon)})
            if len(opts) >= 5:
                break
    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}

# ---------- /tools/lookup_ilan_turu (announcement type) ----------

@router.post("/tools/lookup_ilan_turu")
def tool_lookup_ilan_turu(inp: ToolIlanTuruIn):
    """
    Resolve an announcement type. E.g. {"term":"Kuruluş"} -> {"status":"ok", "code": 28, "name":"LİMİTED ŞİRKET (KURULUŞ)"} (example)
    """
    q = norm_tr(inp.term or "")
    if not q:
        return {"status": "unmapped"}
    code = L.ILAN_TURU_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": int(code), "name": L.ILAN_TURU_NAMES.get(int(code), inp.term)}

    # Suggestions
    opts = []
    for canon, c in L.ILAN_TURU_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": int(c), "name": L.ILAN_TURU_NAMES.get(int(c), canon)})
            if len(opts) >= 5:
                break
    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}

# ---------- /tools/lookup_company (company by name) ----------

@router.post("/tools/lookup_company")
def tool_lookup_company(inp: ToolCompanyIn):
    """
    Resolve a company name to code using:
      1) Exact normalized map (very fast)
      2) Suggestions from a limited scan of vocab (recent-first bias)

    Output:
      - {"status":"ok","code":123,"name":"..."}  exact
      - {"status":"ambiguous","options":[{"code":...,"name":"..."}, ...]} suggestions
      - {"status":"unmapped"}                    nothing found
    """
    raw = (inp.name or "").strip()
    q = norm_tr(raw)
    if not q:
        return {"status": "unmapped"}

    # 1) Exact via normalized map (covers millions of names)
    code = L.UNVAN_NAME_TO_ID.get(q)
    if code is not None:
        try:
            icode = int(code)
        except Exception:
            icode = code
        return {"status": "ok", "code": int(icode), "name": L.UNVAN_VOCAB.get(int(icode), raw)}

    # 2) Suggestions — scan a bounded slice of the vocab for responsiveness.
    #    We bias toward "recent" companies (codes present in RECENT_COMPANY_IDS).
    MAX_SCAN = 100_000   # keep this tight for latency
    MAX_OUT  = 6

    recent_opts: List[Dict] = []
    other_opts:  List[Dict] = []

    # Helper: push into recent vs other buckets
    def push(code_int: int, name_str: str):
        obj = {"code": code_int, "name": name_str}
        if is_recent_company(code_int):
            recent_opts.append(obj)
        else:
            other_opts.append(obj)

    # Iterate a slice of items; we rely on normalization matching
    # either prefix or substring for simple UX.
    scanned = 0
    for cid, name in L.UNVAN_VOCAB.items():
        scanned += 1
        if scanned > MAX_SCAN:
            break
        nn = norm_tr(name)
        if nn.startswith(q) or q in nn:
            push(int(cid), name)
            if len(recent_opts) + len(other_opts) >= (MAX_OUT * 2):  # collect a bit more, then trim
                break

    # Prefer recent first, then fill with others; trim to MAX_OUT
    options = (recent_opts + other_opts)[:MAX_OUT]
    if options:
        return {"status": "ambiguous", "options": options}

    return {"status": "unmapped"}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Minimal public API for the commercial registry dataset — optimized & documented.

Endpoints
---------
GET   /health
POST  /tools/parse_date_range
POST  /tools/lookup_company
POST  /tools/lookup_mudurluk
POST  /tools/lookup_ilan_turu     <-- Added for type lookup (UI/tooling)
POST  /search                     <-- Optimized: most-selective intersection + date union
POST  /answer

Data paths (override via ENV)
-----------------------------
PROJECT_ROOT  -> defaults to CWD
DATA_ROOT     -> ./data
LOOKUP_ROOT   -> ./lookup
INDEX_ROOT    -> ./data/index
SHARDS_ROOT   -> ./data/index_sharded
DOCMETA_BIN   -> ./data/docmeta/docmeta.bin
DOCMETA_META  -> ./data/docmeta/meta.json

Implementation notes
--------------------
- Doc rows are stored in a compact binary (6 x int32 per row), mmapped on startup:
    [date_int, loc_id, type_id, comp_name_code, ad_id, ad_link_code]
- Inverted indexes live as JSON files: date_int / loc_id / type_id (sorted ID lists).
  Optionally sharded as data/index_sharded/<index>/<key>.json for fast single-key loads.
- Search chooses the **most selective** postings among {city, type, date_range}
  (when date_range is selective enough we build a **union** of per-day postings and intersect it too).
- Everything is read-only and process-safe; scale with `--workers N`.
"""

from __future__ import annotations

import os
import json
import struct
import mmap
import re
from datetime import datetime, timedelta, timezone, date
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ──────────────────────────────────────────────────────────────────────────────
# Paths / configuration (override via ENV to deploy in different layouts)
# ──────────────────────────────────────────────────────────────────────────────
ROOT         = os.path.abspath(os.getenv("PROJECT_ROOT", os.getcwd()))
DATA_ROOT    = os.path.abspath(os.getenv("DATA_ROOT",    os.path.join(ROOT, "data")))
LOOKUP_ROOT  = os.path.abspath(os.getenv("LOOKUP_ROOT",  os.path.join(ROOT, "lookup")))
INDEX_ROOT   = os.path.abspath(os.getenv("INDEX_ROOT",   os.path.join(DATA_ROOT, "index")))
SHARDS_ROOT  = os.path.abspath(os.getenv("SHARDS_ROOT",  os.path.join(DATA_ROOT, "index_sharded")))
DOCMETA_BIN  = os.path.abspath(os.getenv("DOCMETA_BIN",  os.path.join(DATA_ROOT, "docmeta", "docmeta.bin")))
DOCMETA_META = os.path.abspath(os.getenv("DOCMETA_META", os.path.join(DATA_ROOT, "docmeta", "meta.json")))

# ──────────────────────────────────────────────────────────────────────────────
# Turkish-aware normalization (used across tools and resolvers)
# ──────────────────────────────────────────────────────────────────────────────
TR_MAP = str.maketrans({
    "ğ":"g","Ğ":"g","ü":"u","Ü":"u","ş":"s","Ş":"s","ı":"i","I":"i","İ":"i","ö":"o","Ö":"o","ç":"c","Ç":"c"
})
def norm_tr(s: str) -> str:
    """Normalize Turkish text for matching: fold accents, lowercase, collapse non-alnum to single space."""
    if not s:
        return ""
    s = s.translate(TR_MAP).lower()
    s = (s.encode("ascii", "ignore")).decode("ascii")  # remove any remaining accents
    out: List[str] = []
    prev_space = False
    for ch in s:
        if ch.isalnum():
            out.append(ch); prev_space = False
        else:
            if not prev_space:
                out.append(" "); prev_space = True
    return " ".join("".join(out).split())

# ──────────────────────────────────────────────────────────────────────────────
# Docmeta binary (6×int32 per row) + mmap wrapper
# Row layout: [date_int, loc_id, type_id, comp_name_code, ad_id, ad_link_code]
# ──────────────────────────────────────────────────────────────────────────────
ROW_INT_COUNT = 6
ROW_SIZE = ROW_INT_COUNT * 4
UNPACK = struct.Struct("<6i").unpack_from
EPOCH_1960 = datetime(1960, 1, 1, tzinfo=timezone.utc)

class DocMeta:
    """Memory-mapped accessor for docmeta.bin. Each row is 6 x int32."""
    def __init__(self, path: str):
        self.path = path
        self._f = open(path, "rb")
        self._mm = mmap.mmap(self._f.fileno(), 0, access=mmap.ACCESS_READ)
        self.rows = self._mm.size() // ROW_SIZE

    def close(self):
        try:
            self._mm.close()
        finally:
            try: self._f.close()
            except Exception: pass

    def get_row(self, rid: int) -> Tuple[int, int, int, int, int, int]:
        """Return row tuple: (date_int, loc_id, type_id, comp_code, ad_id, ad_link_code)."""
        off = rid * ROW_SIZE
        if off < 0 or (off + ROW_SIZE) > self._mm.size():
            raise IndexError("row id out of range")
        return UNPACK(self._mm, off)

DOCMETA: Optional[DocMeta] = None  # set on startup

# ──────────────────────────────────────────────────────────────────────────────
# Lookups: mudurluk (city), ilan_turu (type), unvan (company name vocab)
# ──────────────────────────────────────────────────────────────────────────────
def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default

MUDURLUK_CODES: Dict[str, int] = {}  # normalized name -> code (e.g., "izmir" -> 232)
MUDURLUK_NAMES: Dict[int, str] = {}  # code -> display name
ILAN_TURU_CODES: Dict[str, int] = {} # normalized type name -> code
ILAN_TURU_NAMES: Dict[int, str] = {} # code -> display type name
UNVAN_VOCAB: Dict[int, str] = {}     # company code -> display name
UNVAN_NAME_TO_ID: Dict[str, int] = {}# normalized name -> company code (optional accel)

def load_lookups():
    """Load small vocabularies into RAM."""
    global MUDURLUK_CODES, MUDURLUK_NAMES, ILAN_TURU_CODES, ILAN_TURU_NAMES
    global UNVAN_VOCAB, UNVAN_NAME_TO_ID

    # City/registry offices
    mud = load_json(os.path.join(LOOKUP_ROOT, "mudurluk_codes.json"), {})
    MUDURLUK_CODES = {}
    MUDURLUK_NAMES = {}
    for name, code in mud.items():
        try:
            icode = int(code)
            MUDURLUK_CODES[norm_tr(name)] = icode
            if icode not in MUDURLUK_NAMES:
                MUDURLUK_NAMES[icode] = name
        except Exception:
            continue

    # Announcement types
    typ = load_json(os.path.join(LOOKUP_ROOT, "ilan_turu_codes.json"), {})
    ILAN_TURU_CODES = {}
    ILAN_TURU_NAMES = {}
    for name, code in typ.items():
        try:
            icode = int(code)
            ILAN_TURU_CODES[norm_tr(name)] = icode
            if icode not in ILAN_TURU_NAMES:
                ILAN_TURU_NAMES[icode] = name
        except Exception:
            continue

    # Company vocab
    vocab = load_json(os.path.join(LOOKUP_ROOT, "unvan_vocab.json"), {})
    UNVAN_VOCAB = {}
    for sid, name in vocab.items():
        try:
            UNVAN_VOCAB[int(sid)] = name
        except Exception:
            continue

    # Optional exact-name index for fast company code lookup
    UNVAN_NAME_TO_ID = load_json(os.path.join(LOOKUP_ROOT, "unvan_name_to_id.json"), {})

# ──────────────────────────────────────────────────────────────────────────────
# Inverted indexes: monolithic JSON or sharded JSON per key
# Each postings list is a sorted list of row IDs (ints).
# ──────────────────────────────────────────────────────────────────────────────
def postings_from_shard(index: str, key: int) -> Optional[List[int]]:
    """Load postings for a single key from a shard file if present."""
    shard_file = os.path.join(SHARDS_ROOT, index, f"{key}.json")
    if os.path.exists(shard_file):
        with open(shard_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

@lru_cache(maxsize=8)
def load_monolithic_index(index: str) -> Dict[str, List[int]]:
    """Lazy-load a monolithic index JSON (string keys -> list[int])."""
    path = os.path.join(INDEX_ROOT, f"{index}.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def postings(index: str, key: int) -> List[int]:
    """Get postings for (index, key) as a sorted list of row IDs."""
    arr = postings_from_shard(index, key)
    if arr is not None:
        return arr
    mono = load_monolithic_index(index)
    return mono.get(str(key), [])

# ──────────────────────────────────────────────────────────────────────────────
# Small set ops on sorted lists
# ──────────────────────────────────────────────────────────────────────────────
def intersect_sorted(a: List[int], b: List[int]) -> List[int]:
    """Intersect two sorted int lists into a new list (no side effects)."""
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
    """Union (dedup) two sorted int lists into a new sorted list."""
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
    """Union (dedup) many sorted lists. Cost is linear in total lengths."""
    if not sorted_lists:
        return []
    cur = sorted_lists[0]
    for k in range(1, len(sorted_lists)):
        if not cur:
            break
        cur = union_sorted(cur, sorted_lists[k])
    return cur

# ──────────────────────────────────────────────────────────────────────────────
# Date parsing + helpers (Turkish phrases and YYYY/MM formats)
# ──────────────────────────────────────────────────────────────────────────────
MONTHS_TR = {
    "ocak":1,"subat":2,"şubat":2,"mart":3,"nisan":4,"mayis":5,"mayıs":5,"haziran":6,
    "temmuz":7,"agustos":8,"ağustos":8,"eylul":9,"eylül":9,"ekim":10,"kasim":11,"kasım":11,"aralik":12,"aralık":12
}
RANGE_DOTS = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\s*[.]{2}\s*(\d{4}-\d{2}-\d{2})\s*$")
YEAR_MONTH = re.compile(r"^\s*(\d{4})-(\d{1,2})\s*$")
YEAR_ONLY = re.compile(r"^\s*(\d{4})\s*$")
LAST_N_DAYS = re.compile(r"^\s*son\s+(\d+)\s*g[uü]n\s*$", re.IGNORECASE)

def date_to_sec1960(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int((dt - EPOCH_1960).total_seconds())

def sec1960_to_iso(sec: int) -> str:
    dt = EPOCH_1960 + timedelta(seconds=int(sec))
    return dt.date().isoformat()

def parse_date_range_text(text: str) -> Optional[Tuple[str, str]]:
    """Parse Turkish/ISO date phrases into [from_iso, to_iso]. Returns None if unmapped."""
    s = (text or "").strip()
    if not s:
        return None

    m = LAST_N_DAYS.match(s.lower())
    if m:
        n = max(1, min(3650, int(m.group(1))))
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=n)
        return (start.isoformat(), today.isoformat())

    m = RANGE_DOTS.match(s)
    if m:
        a, b = m.group(1), m.group(2)
        return (a, b) if a <= b else (b, a)

    m = YEAR_MONTH.match(s)
    if m:
        y, mth = int(m.group(1)), int(m.group(2))
        if 1 <= mth <= 12:
            start = date(y, mth, 1)
            end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
            return (start.isoformat(), end.isoformat())

    parts = norm_tr(s).split()
    if len(parts) == 2 and parts[0] in MONTHS_TR and parts[1].isdigit():
        y = int(parts[1]); mth = MONTHS_TR[parts[0]]
        start = date(y, mth, 1)
        end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
        return (start.isoformat(), end.isoformat())

    m = YEAR_ONLY.match(s)
    if m:
        y = int(m.group(1))
        return (date(y, 1, 1).isoformat(), date(y, 12, 31).isoformat())

    return None

# Extra helpers for building date postings keys
DAY_SECS = 86400

def iso_to_sec1960(iso: str) -> int:
    y, m, d = [int(x) for x in iso.split("-")]
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    return int((dt - EPOCH_1960).total_seconds())

def date_keys_for_range(date_from_iso: str, date_to_iso: str) -> List[int]:
    """Return the list of day-aligned date_int keys covering [from, to]."""
    a = iso_to_sec1960(date_from_iso)
    b = iso_to_sec1960(date_to_iso)
    if a > b: a, b = b, a
    keys = []
    x = a - (a % DAY_SECS)
    y = b - (b % DAY_SECS)
    while x <= y:
        keys.append(x)
        x += DAY_SECS
    return keys

# ──────────────────────────────────────────────────────────────────────────────
# Pydantic request models
# ──────────────────────────────────────────────────────────────────────────────
class ToolDateIn(BaseModel):
    text: str

class ToolCompanyIn(BaseModel):
    name: str

class ToolMudurlukIn(BaseModel):
    name: str

class ToolIlanTuruIn(BaseModel):
    term: str

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

class AnswerIn(BaseModel):
    filters: SearchFilters
    q_tr: Optional[str] = None
    max_ctx: Optional[int] = 20

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI app + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Helpbot Dataset API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"],
)

@app.on_event("startup")
def _startup():
    """Pre-load docmeta + lookups at process start (fast, readonly, worker-safe)."""
    global DOCMETA
    if not os.path.exists(DOCMETA_BIN):
        raise RuntimeError(f"docmeta.bin not found: {DOCMETA_BIN}")
    DOCMETA = DocMeta(DOCMETA_BIN)
    load_lookups()

@app.on_event("shutdown")
def _shutdown():
    """Close mmap on process shutdown."""
    if DOCMETA:
        DOCMETA.close()

# ──────────────────────────────────────────────────────────────────────────────
# Endpoints: health + tools
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "ok": True,
        "rows": DOCMETA.rows if DOCMETA else 0,
        "data_root": DATA_ROOT,
        "lookup_root": LOOKUP_ROOT,
        "index_root": INDEX_ROOT,
        "shards_root": SHARDS_ROOT,
    }

@app.post("/tools/parse_date_range")
def tool_parse_date_range(inp: ToolDateIn):
    rng = parse_date_range_text(inp.text or "")
    if not rng:
        return {"status": "unmapped"}
    return {"status": "ok", "range": {"from": rng[0], "to": rng[1]}}

@app.post("/tools/lookup_mudurluk")
def tool_lookup_mudurluk(inp: ToolMudurlukIn):
    """Resolve a city/registry office by normalized text. Returns ok/ambiguous/unmapped."""
    q = norm_tr(inp.name or "")
    if not q:
        return {"status": "unmapped"}
    code = MUDURLUK_CODES.get(q)
    if code is not None:
        return {"status": "ok", "code": code, "name": MUDURLUK_NAMES.get(code, inp.name)}
    # heuristic suggestions
    opts = []
    for canon, c in MUDURLUK_CODES.items():
        if canon.startswith(q) or q in canon:
            opts.append({"code": c, "name": MUDURLUK_NAMES.get(c, canon)})
            if len(opts) >= 5:
                break
    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}

@app.post("/tools/lookup_ilan_turu")
def tool_lookup_ilan_turu(inp: ToolIlanTuruIn):
    """Resolve an announcement type by text (e.g., 'Kuruluş')."""
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
            if len(opts) >= 5:
                break
    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}

@app.post("/tools/lookup_company")
def tool_lookup_company(inp: ToolCompanyIn):
    """Resolve company by name (uses exact-name index if present; otherwise a cheap scan slice)."""
    q = norm_tr(inp.name or "")
    if not q:
        return {"status": "unmapped"}

    # Fast path: exact-name index (norm -> id)
    if UNVAN_NAME_TO_ID:
        code = UNVAN_NAME_TO_ID.get(q)
        if code:
            try: icode = int(code)
            except Exception: icode = code
            return {"status": "ok", "code": int(icode), "name": UNVAN_VOCAB.get(int(icode), inp.name)}
        # Fallback suggestions
        opts = []
        for nname, cid in UNVAN_NAME_TO_ID.items():
            if nname.startswith(q) or q in nname:
                try: icode = int(cid)
                except Exception: continue
                opts.append({"code": icode, "name": UNVAN_VOCAB.get(icode, "")})
                if len(opts) >= 5:
                    break
        if opts:
            return {"status": "ambiguous", "options": opts}
        return {"status": "unmapped"}

    # Slow path (only small slice): scan first 50k for suggestions
    for cid, name in UNVAN_VOCAB.items():
        if norm_tr(name) == q:
            return {"status": "ok", "code": int(cid), "name": name}
    opts = []
    for cid, name in list(UNVAN_VOCAB.items())[:50000]:
        nn = norm_tr(name)
        if nn.startswith(q) or q in nn:
            opts.append({"code": int(cid), "name": name})
            if len(opts) >= 5:
                break
    if opts:
        return {"status": "ambiguous", "options": opts}
    return {"status": "unmapped"}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers for hydrating rows and building candidate sets
# ──────────────────────────────────────────────────────────────────────────────
def hydrate_row(rid: int) -> dict:
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
        "ad_link": "",  # if you later expose ad_link code → URL mapping, fill here
    }

# ──────────────────────────────────────────────────────────────────────────────
# /search — Optimized: choose most-selective lists + optional date-union
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/search")
def search(inp: SearchIn):
    """
    Perform an indexed search. Strategy:
      1) Build postings lists for city/type.
      2) If a date range is provided, consider building a union of daily postings
         (only if it's reasonably selective; otherwise apply as a post-filter).
      3) Sort lists by size (smallest first) and intersect.
      4) Apply remaining filters (date post-filter and/or company_code), hydrate and return.
    """
    f = inp.filters or SearchFilters()
    limit = max(1, min(200, inp.limit or f.limit or 40))

    lists: List[Tuple[str, List[int]]] = []   # [(label, list)]
    city_list: Optional[List[int]] = None
    type_list: Optional[List[int]] = None
    date_list: Optional[List[int]] = None

    # 1) City postings
    if f.city_code is not None:
        city_list = postings("loc_id", int(f.city_code))
        lists.append(("city", city_list))

    # 2) Type postings
    if f.type_code is not None:
        type_list = postings("type_id", int(f.type_code))
        lists.append(("type", type_list))

    # 3) Date range → union of per-day postings (if selective enough)
    have_date_range = bool(f.date_from and f.date_to)
    date_keys: List[int] = []
    if have_date_range:
        try:
            date_keys = date_keys_for_range(f.date_from, f.date_to)
            # Load per-day lists; estimate total candidates if we union them
            day_lists = [postings("date_int", k) for k in date_keys]
            est = sum(len(x) for x in day_lists)

            # Heuristic: include date union if it's likely to reduce candidates.
            # Compare to city/type sizes (if they exist); if est is within a multiple, include it.
            bases = []
            if city_list is not None: bases.append(len(city_list))
            if type_list is not None: bases.append(len(type_list))
            min_base = min(bases) if bases else est
            # Multiplier controls aggressiveness; 4 is a good default for 30-60 day windows.
            if est <= (min_base * 4):
                date_list = union_many(day_lists)
                lists.append(("date", date_list))
        except Exception:
            # fall back to post-filter by date if anything goes wrong
            date_list = None

    if not lists:
        # pure date-range search (no city/type) is allowed if date_list exists
        if date_list is not None:
            lists.append(("date", date_list))
        else:
            raise HTTPException(status_code=400, detail="Provide at least one of: city_code, type_code, or a valid date range.")

    # 4) Intersect lists from smallest to largest
    lists.sort(key=lambda t: len(t[1]))
    cur = lists[0][1]
    for _, arr in lists[1:]:
        if not cur or not arr:
            cur = []
            break
        cur = intersect_sorted(cur, arr)
        if not cur:
            break

    # Determine if we still need to post-filter by date (when we skipped building date_list)
    need_date_post_filter = have_date_range and (date_list is None)
    if need_date_post_filter:
        di_from = iso_to_sec1960(f.date_from)
        di_to   = iso_to_sec1960(f.date_to)

    # 5) Stream candidates and finalize
    hits: List[dict] = []
    for rid in cur:
        di, loc, typ, comp_code, adid, _ = DOCMETA.get_row(rid)

        if need_date_post_filter and (di < di_from or di > di_to):
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

# ──────────────────────────────────────────────────────────────────────────────
# /answer — Deterministic summarizer over the first N hits (no LLM)
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/answer")
def answer(inp: AnswerIn):
    s = search(SearchIn(filters=inp.filters, limit=min(inp.max_ctx or 20, 100)))
    hits = s.get("hits", [])
    if not hits:
        return {"answer_tr": "Sonuç bulunamadı.", "sources": []}

    lines: List[str] = []
    for h in hits:
        iso = sec1960_to_iso(h.get("date_int", 0))
        lines.append(f"- [{h.get('ad_id','')}] {iso} • {h.get('city','')} • {h.get('type','')} • {h.get('company','')}")

    q = (inp.q_tr or "").strip()
    summary: List[str] = []
    if q:
        summary.append(f"Soru: {q}")
    summary.append(f"Sonuç sayısı (ilk {len(hits)} gösteriliyor): {len(hits)}")
    summary.extend(lines)
    return {"answer_tr": "\n".join(summary), "sources": hits}

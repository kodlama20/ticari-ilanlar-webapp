#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
routes_answer.py
================

Implements the /answer endpoint: a deterministic, no-LLM summary over the
first N hits of the same search logic.

We call `ensure_docmeta_loaded()` to guarantee mmap availability, and we
reuse the /search function via import to avoid duplicating logic.
"""

from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter
from pydantic import BaseModel

from .docmeta import ensure_docmeta_loaded
from .routes_search import search as search_endpoint, SearchIn, SearchFilters

# Small helper to convert date_int → YYYY-MM-DD for display
from datetime import datetime, timezone, timedelta
EPOCH_1960 = datetime(1960, 1, 1, tzinfo=timezone.utc)
def _sec1960_to_iso(sec: int) -> str:
    dt = EPOCH_1960 + timedelta(seconds=int(sec))
    return dt.date().isoformat()


router = APIRouter()


# Keep model structure identical to your previous implementation
class AnswerIn(BaseModel):
    filters: SearchFilters
    q_tr: Optional[str] = None
    max_ctx: Optional[int] = 20


@router.post("/answer")
def answer(inp: AnswerIn):
    """
    Deterministic summary:
      - runs the same search
      - formats first N results into a short list
    """
    ensure_docmeta_loaded()  # lazy open (or HTTP 503)

    s = search_endpoint(SearchIn(filters=inp.filters, limit=min(inp.max_ctx or 20, 100)))
    hits = s.get("hits", [])
    if not hits:
        return {"answer_tr": "Sonuç bulunamadı.", "sources": []}

    lines: List[str] = []
    for h in hits:
        iso = _sec1960_to_iso(h.get("date_int", 0))
        lines.append(f"- [{h.get('ad_id','')}] {iso} • {h.get('city','')} • {h.get('type','')} • {h.get('company','')}")

    q = (inp.q_tr or "").strip()
    summary: List[str] = []
    if q:
        summary.append(f"Soru: {q}")
    summary.append(f"Sonuç sayısı (ilk {len(hits)} gösteriliyor): {len(hits)}")
    summary.extend(lines)
    return {"answer_tr": "\n".join(summary), "sources": hits}

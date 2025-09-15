#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""GET /health endpoint."""

from fastapi import APIRouter
from .config import DATA_ROOT, LOOKUP_ROOT, INDEX_ROOT, SHARDS_ROOT
from .docmeta import DOCMETA

router = APIRouter()

@router.get("/health")
def health():
    return {
        "ok": True,
        "rows": DOCMETA.rows if DOCMETA else 0,
        "data_root": DATA_ROOT,
        "lookup_root": LOOKUP_ROOT,
        "index_root": INDEX_ROOT,
        "shards_root": SHARDS_ROOT,
    }

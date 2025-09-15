# server/app.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI application assembly (modular, commented for public sharing).

- Creates the ASGI `app`
- CORS for a static UI
- Startup:
    * mmap docmeta
    * load lookups (mutate in place)
    * load recent company ids (optional precomputed list)
- /health shows lookup counts (including recent companies)
- Mounts route groups: /tools/*, /search, /answer
"""

from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Centralized config/paths
from .config import DATA_ROOT, LOOKUP_ROOT, INDEX_ROOT, SHARDS_ROOT, DOCMETA_BIN

# Docmeta: module exposes a global DOCMETA we set on startup
from . import docmeta as DM

# Lookups: import the module (so we always read latest dicts the module owns)
from . import lookups as L

# Recency: **import as a module** so reassignment inside recency is visible here
from . import recency as R

# Routers
from .routes_health import router as health_router
from .routes_tools  import router as tools_router
from .routes_search import router as search_router
from .routes_answer import router as answer_router


app = FastAPI(title="Helpbot Dataset API", version="1.3.1")

# Open CORS (relax here for testing; restrict origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    """
    Load memory-mapped docmeta and lookup dictionaries, then load recent companies.
    We import modules above and mutate their module-level globals here.
    """
    # 1) Docmeta mmap
    if not os.path.exists(DOCMETA_BIN):
        raise RuntimeError(f"docmeta.bin not found: {DOCMETA_BIN}")
    DM.DOCMETA = DM.DocMeta(DOCMETA_BIN)

    # 2) Lookups (cities/types/companies)
    L.load_lookups(LOOKUP_ROOT)

    # 3) Recent companies (optional precomputed JSON file)
    #    We pass LOOKUP_ROOT explicitly so the file is found at:
    #       {LOOKUP_ROOT}/recent_company_ids.json
    R.load_recent_companies(lookup_root=LOOKUP_ROOT)


@app.on_event("shutdown")
def _shutdown() -> None:
    """Close mmap on shutdown."""
    if DM.DOCMETA:
        DM.DOCMETA.close()


@app.get("/health")
def health():
    """
    Show basic service status + where files are mounted + counts for lookups.
    IMPORTANT: read lookup dicts via their MODULE (L.*) and recency via MODULE (R.*)
    so we see the latest objects even if those modules reassign their globals.
    """
    return {
        "ok": True,
        "rows": int(getattr(DM.DOCMETA, "rows", 0) or 0),
        "data_root":   DATA_ROOT,
        "lookup_root": LOOKUP_ROOT,
        "index_root":  INDEX_ROOT,
        "shards_root": SHARDS_ROOT,
        "lookup_counts": {
            "cities":           len(L.MUDURLUK_CODES),
            "types":            len(L.ILAN_TURU_CODES),
            "companies_vocab":  len(L.UNVAN_VOCAB),
            "companies_map":    len(L.UNVAN_NAME_TO_ID),
            "recent_companies": len(R.RECENT_COMPANY_IDS),  # <- will now be non-zero
        },
    }


# Mount endpoint groups
app.include_router(health_router)
app.include_router(tools_router)
app.include_router(search_router)
app.include_router(answer_router)

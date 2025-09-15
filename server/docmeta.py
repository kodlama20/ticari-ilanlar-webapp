#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
docmeta.py
==========

Single place to open/hold the memory-mapped `docmeta.bin`.

- Row layout is 6 x int32, little-endian:
    [date_int, loc_id, type_id, comp_name_code, ad_id, ad_link_code]

- Use `ensure_docmeta_loaded()` wherever you need to read rows. It lazily
  opens the mmap on first use (or raises HTTP 503 if the file is missing).

This keeps startup resilient (the app can boot even if the file is missing),
and every route becomes robust by calling the loader before accessing rows.
"""

from __future__ import annotations

import os
import mmap
import struct
from typing import Optional, Tuple

from fastapi import HTTPException

from .config import DOCMETA_BIN

# ---------- Binary layout ----------
ROW_INT_COUNT = 6
ROW_SIZE = ROW_INT_COUNT * 4
UNPACK = struct.Struct("<6i").unpack_from  # little-endian 6x int32

class DocMeta:
    """Memory-mapped accessor for docmeta.bin."""

    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        self.path = path
        self._f = open(path, "rb")
        self._mm = mmap.mmap(self._f.fileno(), 0, access=mmap.ACCESS_READ)
        self.rows = self._mm.size() // ROW_SIZE

    def get_row(self, rid: int) -> Tuple[int, int, int, int, int, int]:
        """
        Return a row tuple:
            (date_int, loc_id, type_id, comp_code, ad_id, ad_link_code)
        """
        off = rid * ROW_SIZE
        if off < 0 or (off + ROW_SIZE) > self._mm.size():
            raise IndexError("row id out of range")
        return UNPACK(self._mm, off)

    def close(self) -> None:
        try:
            self._mm.close()
        finally:
            try:
                self._f.close()
            except Exception:
                pass


# Module-level singleton (set lazily).
DOCMETA: Optional[DocMeta] = None


def open_docmeta(path: str) -> DocMeta:
    """Open the mmap and return a DocMeta instance."""
    return DocMeta(path)


def ensure_docmeta_loaded() -> DocMeta:
    """
    Lazy-load the global DOCMETA. Call this at the start of any route or
    helper that needs row access.

    Raises:
        HTTPException(503) if the file is not available.
    """
    global DOCMETA
    if DOCMETA is None:
        if not os.path.exists(DOCMETA_BIN):
            # Return an HTTP-layer error rather than crashing the process.
            raise HTTPException(status_code=503, detail="Docmeta not loaded.")
        DOCMETA = open_docmeta(DOCMETA_BIN)
    return DOCMETA


def close_docmeta() -> None:
    """Close the mmap (used by app shutdown)."""
    global DOCMETA
    if DOCMETA:
        DOCMETA.close()
        DOCMETA = None

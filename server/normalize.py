#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Turkish-aware normalization used across all lookups and tools.
"""

from typing import List

_TR_MAP = str.maketrans({
    "ğ":"g","Ğ":"g","ü":"u","Ü":"u","ş":"s","Ş":"s","ı":"i","I":"i","İ":"i","ö":"o","Ö":"o","ç":"c","Ç":"c"
})

def norm_tr(s: str) -> str:
    """
    Fold Turkish accents, lower, remove non-alnum to single spaces, collapse.
    """
    if not s:
        return ""
    s = s.translate(_TR_MAP).lower()
    s = (s.encode("ascii", "ignore")).decode("ascii")  # strip leftovers
    out: List[str] = []
    prev_space = False
    for ch in s:
        if ch.isalnum():
            out.append(ch); prev_space = False
        else:
            if not prev_space:
                out.append(" "); prev_space = True
    return " ".join("".join(out).split())

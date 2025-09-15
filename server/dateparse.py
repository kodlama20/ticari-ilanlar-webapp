#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Date range parser for Turkish phrases:
- "son 30 gün", "son 5 yil" / "son 5 yıl"
- "2024-01..2024-03"
- "Ocak 2025", "2019-7", "2019"
"""

import re
from datetime import datetime, timedelta, timezone, date
from .normalize import norm_tr

MONTHS_TR = {
    "ocak":1,"subat":2,"subat":2,"mart":3,"nisan":4,"mayis":5,"mayis":5,"haziran":6,
    "temmuz":7,"agustos":8,"agustos":8,"eylul":9,"eylul":9,"ekim":10,"kasim":11,"kasim":11,"aralik":12,"aralik":12
}

RE_RANGE_DOTS  = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\s*[.]{2}\s*(\d{4}-\d{2}-\d{2})\s*$")
RE_YEAR_MONTH  = re.compile(r"^\s*(\d{4})-(\d{1,2})\s*$")
RE_YEAR_ONLY   = re.compile(r"^\s*(\d{4})\s*$")
RE_LAST_DAYS   = re.compile(r"^\s*son\s+(\d+)\s*g[uü]n\s*$", re.IGNORECASE)
RE_LAST_YEARS  = re.compile(r"^\s*son\s+(\d+)\s*y[iı]l\s*$", re.IGNORECASE)

def parse_date_range_text(text: str):
    """
    Return (from_iso, to_iso) or None if unmapped.
    """
    s = (text or "").strip()
    if not s:
        return None

    # son N gün
    m = RE_LAST_DAYS.match(s.lower())
    if m:
        n = max(1, min(3650, int(m.group(1))))
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=n)
        return (start.isoformat(), today.isoformat())

    # son N yıl
    m = RE_LAST_YEARS.match(s.lower())
    if m:
        n = max(1, min(200, int(m.group(1))))
        today = datetime.now(timezone.utc).date()
        start = date(today.year - n, today.month, today.day)
        return (start.isoformat(), today.isoformat())

    # 2024-01..2024-03
    m = RE_RANGE_DOTS.match(s)
    if m:
        a, b = m.group(1), m.group(2)
        return (a, b) if a <= b else (b, a)

    # 2019-7 or 2019-07
    m = RE_YEAR_MONTH.match(s)
    if m:
        y, mth = int(m.group(1)), int(m.group(2))
        if 1 <= mth <= 12:
            start = date(y, mth, 1)
            end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
            return (start.isoformat(), end.isoformat())

    # Ocak 2025
    parts = norm_tr(s).split()
    if len(parts) == 2 and parts[0] in MONTHS_TR and parts[1].isdigit():
        y = int(parts[1]); mth = MONTHS_TR[parts[0]]
        start = date(y, mth, 1)
        end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
        return (start.isoformat(), end.isoformat())

    # 2019
    m = RE_YEAR_ONLY.match(s)
    if m:
        y = int(m.group(1))
        return (date(y, 1, 1).isoformat(), date(y, 12, 31).isoformat())

    return None

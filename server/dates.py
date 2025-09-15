#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Date parsing & conversions for the dataset.
Supports:
- "son N gün"
- "son N yıl"  ← NEW
- "YYYY-MM-DD..YYYY-MM-DD"
- "YYYY-MM"
- "Ocak 2025" (TR month + year)
- "2019"
Also provides conversions between ISO dates and seconds since 1960-01-01 (dataset key).
"""

import re
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Tuple

from .normalize import norm_tr

EPOCH_1960 = datetime(1960, 1, 1, tzinfo=timezone.utc)
DAY_SECS = 86400

MONTHS_TR = {
    "ocak":1,"subat":2,"şubat":2,"mart":3,"nisan":4,"mayis":5,"mayıs":5,"haziran":6,
    "temmuz":7,"agustos":8,"ağustos":8,"eylul":9,"eylül":9,"ekim":10,"kasim":11,"kasım":11,"aralik":12,"aralık":12
}

RANGE_DOTS   = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\s*[.]{2}\s*(\d{4}-\d{2}-\d{2})\s*$")
YEAR_MONTH   = re.compile(r"^\s*(\d{4})-(\d{1,2})\s*$")
YEAR_ONLY    = re.compile(r"^\s*(\d{4})\s*$")
LAST_N_DAYS  = re.compile(r"^\s*son\s+(\d+)\s*g[uü]n\s*$", re.IGNORECASE)
LAST_N_YEARS = re.compile(r"^\s*son\s+(\d+)\s*y[iı]l\s*$", re.IGNORECASE)  # NEW

def date_to_sec1960(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int((dt - EPOCH_1960).total_seconds())

def sec1960_to_iso(sec: int) -> str:
    dt = EPOCH_1960 + timedelta(seconds=int(sec))
    return dt.date().isoformat()

def iso_to_sec1960(iso: str) -> int:
    y, m, d = [int(x) for x in iso.split("-")]
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    return int((dt - EPOCH_1960).total_seconds())

def date_keys_for_range(date_from_iso: str, date_to_iso: str) -> List[int]:
    """Return day-aligned date_int keys covering [from, to]."""
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

def parse_date_range_text(text: str) -> Optional[Tuple[str, str]]:
    """Parse Turkish/ISO date phrases into (from_iso, to_iso). None if unmapped."""
    s = (text or "").strip()
    if not s:
        return None

    # "son N gün"
    m = LAST_N_DAYS.match(s.lower())
    if m:
        n = max(1, min(3650, int(m.group(1))))
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=n)
        return (start.isoformat(), today.isoformat())

    # "son N yıl" (NEW)
    m = LAST_N_YEARS.match(s.lower())
    if m:
        n = max(1, min(200, int(m.group(1))))
        today = datetime.now(timezone.utc).date()
        # Try to go back N years; if leap day mismatch occurs, back off one day first.
        try:
            start = today.replace(year=today.year - n)
        except ValueError:
            start = (today - timedelta(days=1)).replace(year=today.year - n)
        return (start.isoformat(), today.isoformat())

    # "YYYY-MM-DD..YYYY-MM-DD"
    m = RANGE_DOTS.match(s)
    if m:
        a, b = m.group(1), m.group(2)
        return (a, b) if a <= b else (b, a)

    # "YYYY-MM"
    m = YEAR_MONTH.match(s)
    if m:
        y, mth = int(m.group(1)), int(m.group(2))
        if 1 <= mth <= 12:
            start = date(y, mth, 1)
            end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
            return (start.isoformat(), end.isoformat())

    # "Ocak 2025"
    parts = norm_tr(s).split()
    if len(parts) == 2 and parts[0] in MONTHS_TR and parts[1].isdigit():
        y = int(parts[1]); mth = MONTHS_TR[parts[0]]
        start = date(y, mth, 1)
        end = (date(y+1, 1, 1) - timedelta(days=1)) if mth == 12 else (date(y, mth+1, 1) - timedelta(days=1))
        return (start.isoformat(), end.isoformat())

    # "2019"
    m = YEAR_ONLY.match(s)
    if m:
        y = int(m.group(1))
        return (date(y, 1, 1).isoformat(), date(y, 12, 31).isoformat())

    return None

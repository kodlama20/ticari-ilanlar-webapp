#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Small helpers for working with sorted integer ID lists."""

from typing import List

def intersect_sorted(a: List[int], b: List[int]) -> List[int]:
    """Intersect two sorted int lists into a new list."""
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
    """Union (dedup) many sorted lists, linear in total lengths."""
    if not sorted_lists:
        return []
    cur = sorted_lists[0]
    for k in range(1, len(sorted_lists)):
        if not cur:
            break
        cur = union_sorted(cur, sorted_lists[k])
    return cur

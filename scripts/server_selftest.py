#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Server self-test for the Helpbot Dataset API.

What it does
------------
1) Sanity checks: /health, tools endpoints.
2) Realistic data flow:
   - Parse a date window ("son 30 gün")
   - Pick several city codes from lookup/mudurluk_codes.json
   - Search per city to harvest valid company codes/names
   - Validate that each /search hit satisfies requested filters
3) Company & city resolvers:
   - Test normalization (e.g., "canakkale" vs "çanakkale")
   - Test ambiguous suggestions (short prefixes)
4) Performance:
   - Latency stats (avg, p95) per endpoint
   - Optional concurrent /search load
5) Summary:
   - PASS/FAIL with details

Requirements
-----------
- Python 3.9+
- Recommended: `pip install httpx` for concurrency.
  Falls back to `requests` (sequential) if httpx is not installed.

Run
---
python scripts/server_selftest.py \
  --base http://localhost:8000 \
  --lookup ./lookup \
  --cities 8 \
  --per-city 3 \
  --concurrency 12 \
  --search-runs 40 \
  --assert-search-ms 600 \
  --assert-answer-ms 800

Tip: Point --base to your public URL to test from outside network.
"""

import argparse, json, os, random, statistics, time
from datetime import datetime, timezone, timedelta, date
from typing import Dict, List, Optional, Tuple

# Optional httpx for concurrency
try:
    import httpx
    HAVE_HTTPX = True
except Exception:
    HAVE_HTTPX = False

# Fallback to requests for sequential mode
try:
    import requests
    HAVE_REQUESTS = True
except Exception:
    HAVE_REQUESTS = False

# ---------- TR normalization (align with server) ----------
_TR_MAP = str.maketrans({
    "ğ":"g","Ğ":"g","ü":"u","Ü":"u","ş":"s","Ş":"s","ı":"i","I":"i","İ":"i","ö":"o","Ö":"o","ç":"c","Ç":"c"
})
def norm_tr(s: str) -> str:
    if not s: return ""
    s = s.translate(_TR_MAP).lower()
    s = (s.encode("ascii","ignore")).decode("ascii")
    out = []
    prev_space = False
    for ch in s:
        if ch.isalnum(): out.append(ch); prev_space = False
        else:
            if not prev_space: out.append(" "); prev_space = True
    return " ".join("".join(out).split())

# ---------- Date helpers (align with server) ----------
E1960 = datetime(1960,1,1,tzinfo=timezone.utc)
def sec1960_to_dateiso(sec: int) -> str:
    d = E1960 + timedelta(seconds=int(sec))
    return d.date().isoformat()

# ---------- HTTP helpers ----------
def _join(base: str, path: str) -> str:
    return base.rstrip("/") + path

def post_json_requests(base: str, path: str, payload: dict) -> Tuple[dict, float]:
    url = _join(base, path)
    t0 = time.perf_counter()
    r = requests.post(url, json=payload, timeout=60)
    dt = (time.perf_counter() - t0) * 1000.0
    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"{url} returned non-JSON: {r.text[:160]}...")
    if not r.ok:
        raise RuntimeError(f"{url} -> HTTP {r.status_code}: {j}")
    return j, dt

async def post_json_httpx(client: "httpx.AsyncClient", base: str, path: str, payload: dict) -> Tuple[dict, float]:
    url = _join(base, path)
    t0 = time.perf_counter()
    r = await client.post(url, json=payload, timeout=60.0)
    dt = (time.perf_counter() - t0) * 1000.0
    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"{url} returned non-JSON: {r.text[:160]}...")
    if r.status_code >= 400:
        raise RuntimeError(f"{url} -> HTTP {r.status_code}: {j}")
    return j, dt

# ---------- Test runner ----------
class Runner:
    def __init__(self, base: str, lookup_dir: str, seed: int = 42):
        self.base = base.rstrip("/")
        self.lookup_dir = lookup_dir
        random.seed(seed)
        self.times = {
            "parse_date_range": [],
            "lookup_city": [],
            "lookup_company": [],
            "search": [],
            "answer": [],
        }
        self.failures: List[str] = []
        self.notes: List[str] = []

        # Load small lookup (mudurluk_codes is small; others optional)
        self.mudurluk_codes: Dict[str,int] = self._load_json(os.path.join(lookup_dir, "mudurluk_codes.json"), {})
        if not self.mudurluk_codes:
            self.failures.append("lookup/mudurluk_codes.json not found or empty")

    def _load_json(self, p: str, default):
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default

    # ---- basic HTTP using requests (fallback) ----
    def post(self, path: str, payload: dict) -> Tuple[dict, float]:
        if not HAVE_REQUESTS:
            raise RuntimeError("requests not installed. Install httpx and use concurrency mode, or `pip install requests`.")
        return post_json_requests(self.base, path, payload)

    # ---- tests ----
    def test_health(self):
        # health via requests (GET)
        import urllib.request, json as _json
        url = _join(self.base, "/health")
        t0 = time.perf_counter()
        with urllib.request.urlopen(url, timeout=30) as resp:
            body = resp.read()
        dt = (time.perf_counter() - t0) * 1000.0
        try:
            j = _json.loads(body.decode("utf-8", errors="ignore"))
        except Exception:
            raise RuntimeError("/health returned non-JSON")
        if not j.get("ok"):
            raise RuntimeError("/health ok=false")
        if int(j.get("rows", 0)) <= 0:
            self.failures.append("health.rows is zero")
        self.notes.append(f"/health ok (rows={j.get('rows',0)}, {dt:.1f} ms)")

    def test_parse_date_range(self):
        cases = [
            "Ocak 2025",
            "2024-01-01..2024-03-31",
            "son 7 gün",
            "2023",
            "2025-02",
            "  ",
        ]
        for c in cases:
            j, dt = self.post("/tools/parse_date_range", {"text": c})
            self.times["parse_date_range"].append(dt)
            if c.strip() == "":
                if j.get("status") not in ("unmapped",):
                    self.failures.append(f"parse_date_range('{c}') should be unmapped")
            else:
                if j.get("status") != "ok":
                    self.failures.append(f"parse_date_range('{c}') not ok -> {j}")
                else:
                    r = j.get("range", {})
                    if not (r.get("from") and r.get("to")):
                        self.failures.append(f"parse_date_range('{c}') missing range: {j}")

    def pick_sample_cities(self, k: int) -> List[Tuple[str,int]]:
        pairs = []
        # mudurluk_codes: name->code
        items = list(self.mudurluk_codes.items())
        if not items:
            return pairs
        random.shuffle(items)
        for name, code in items:
            pairs.append((name, int(code)))
            if len(pairs) >= k:
                break
        return pairs

    def test_lookup_city(self, names: List[str]):
        for nm in names:
            # direct
            j, dt = self.post("/tools/lookup_mudurluk", {"name": nm})
            self.times["lookup_city"].append(dt)
            st = j.get("status")
            if st not in ("ok","ambiguous","unmapped"):
                self.failures.append(f"lookup_mudurluk('{nm}') invalid status: {st}")
            # normalization variant
            nm2 = norm_tr(nm).replace("c","ç").replace("i","ı")  # intentionally messy
            j2, dt2 = self.post("/tools/lookup_mudurluk", {"name": nm2})
            self.times["lookup_city"].append(dt2)
            st2 = j2.get("status")
            if st2 not in ("ok","ambiguous","unmapped"):
                self.failures.append(f"lookup_mudurluk('{nm2}') invalid status: {st2}")

    def search_and_harvest_companies(self, city_code: int, date_from: str, date_to: str, per_city: int) -> List[dict]:
        companies = []
        j, dt = self.post("/search", {"filters": {"city_code": city_code, "date_from": date_from, "date_to": date_to}, "limit": max(5, per_city*5)})
        self.times["search"].append(dt)
        hits = j.get("hits", [])
        for h in hits:
            companies.append({
                "code": int(h.get("comp_name", 0)),
                "name": h.get("company",""),
            })
            if len(companies) >= per_city:
                break
        return companies, hits

    def validate_hits(self, hits: List[dict], expect_city: Optional[int], di_from: Optional[int], di_to: Optional[int], expect_company: Optional[int] = None):
        for h in hits:
            if expect_city is not None and int(h.get("loc_id")) != int(expect_city):
                self.failures.append(f"/search hit city mismatch: got {h.get('loc_id')} expected {expect_city}")
            if di_from is not None:
                di = int(h.get("date_int", 0))
                if not (di_from <= di <= di_to):
                    self.failures.append(f"/search hit date out of range: {di} ({sec1960_to_dateiso(di)})")
            if expect_company is not None and int(h.get("comp_name", -1)) != int(expect_company):
                self.failures.append(f"/search hit company mismatch: got {h.get('comp_name')} expected {expect_company}")

    def test_answer(self, filt: dict):
        j, dt = self.post("/answer", {"filters": filt, "q_tr": "test", "max_ctx": 15})
        self.times["answer"].append(dt)
        if "answer_tr" not in j:
            self.failures.append("/answer missing answer_tr")

    # ---------- Overall run ----------
    def run(self, cities: int, per_city: int, search_runs: int, concurrency: int,
            assert_search_ms: Optional[float], assert_answer_ms: Optional[float]):
        # 1) health
        self.test_health()

        # 2) parse date range ("son 30 gün")
        r, dt = self.post("/tools/parse_date_range", {"text":"son 30 gün"})
        self.times["parse_date_range"].append(dt)
        if r.get("status") != "ok":
            self.failures.append("parse_date_range('son 30 gün') not ok")
            return
        date_from = r["range"]["from"]
        date_to   = r["range"]["to"]

        # 3) city resolver tests (direct + messy)
        pairs = self.pick_sample_cities(max(4, cities))
        self.test_lookup_city([name for name,_ in pairs[:min(10,len(pairs))]])

        # 4) per-city searches + harvest companies
        all_companies = []
        city_codes = []
        for name, code in pairs[:cities]:
            comps, hits = self.search_and_harvest_companies(code, date_from, date_to, per_city)
            # Validate those hits
            # Convert date to seconds to check
            di_from = self._iso_to_sec1960(date_from)
            di_to   = self._iso_to_sec1960(date_to)
            self.validate_hits(hits, code, di_from, di_to, None)
            all_companies.extend([(code, c["code"], c["name"]) for c in comps if c["code"]])
            city_codes.append(code)

        if not all_companies:
            self.notes.append("No companies harvested from searches; company lookup tests may be limited.")

        # 5) company resolver tests using harvested names
        names_for_test = [nm for _,_,nm in all_companies if nm][:min(20, len(all_companies))]
        for nm in names_for_test:
            # direct
            j, dt = self.post("/tools/lookup_company", {"name": nm})
            self.times["lookup_company"].append(dt)
            st = j.get("status")
            if st not in ("ok","ambiguous","unmapped"):
                self.failures.append(f"lookup_company('{nm}') invalid status: {st}")
            # normalized
            messy = nm.upper().replace("I","İ").replace("C","Ç")
            j2, dt2 = self.post("/tools/lookup_company", {"name": messy})
            self.times["lookup_company"].append(dt2)
            st2 = j2.get("status")
            if st2 not in ("ok","ambiguous","unmapped"):
                self.failures.append(f"lookup_company('{messy}') invalid status: {st2}")

        # 6) accuracy: pick random harvested company and verify search filter-by-company
        if all_companies:
            for _ in range(min(8, len(all_companies))):
                ccode = random.choice(all_companies)
                city_code = ccode[0]
                company_code = ccode[1]
                j, dt = self.post("/search", {"filters":{"city_code": city_code, "company_code": company_code, "date_from": date_from, "date_to": date_to}, "limit": 20})
                self.times["search"].append(dt)
                hits = j.get("hits", [])
                di_from = self._iso_to_sec1960(date_from); di_to = self._iso_to_sec1960(date_to)
                self.validate_hits(hits, city_code, di_from, di_to, company_code)

        # 7) /answer latency and presence
        if city_codes:
            filt = {"city_code": city_codes[0], "date_from": date_from, "date_to": date_to}
            self.test_answer(filt)

        # 8) Concurrency load test on /search
        if search_runs > 0:
            self.load_test_search(city_codes or [pairs[0][1] if pairs else 0], date_from, date_to, search_runs, concurrency)

        # 9) asserts
        self._assert_perf("search", assert_search_ms)
        self._assert_perf("answer", assert_answer_ms)

        # Summary
        self.print_summary()

    def load_test_search(self, city_codes: List[int], date_from: str, date_to: str, total_runs: int, concurrency: int):
        if total_runs <= 0:
            return
        if HAVE_HTTPX:
            # concurrent using httpx
            import asyncio
            async def _run():
                limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency)
                async with httpx.AsyncClient(limits=limits, timeout=60.0) as client:
                    tasks = []
                    for i in range(total_runs):
                        city_code = int(random.choice(city_codes))
                        payload = {"filters":{"city_code":city_code, "date_from":date_from, "date_to":date_to},"limit":10}
                        tasks.append(self._search_httpx(client, payload))
                        if len(tasks) >= concurrency:
                            await asyncio.gather(*tasks)
                            tasks = []
                    if tasks:
                        await asyncio.gather(*tasks)
            asyncio.run(_run())
        else:
            # sequential fallback
            self.notes.append("httpx not installed; running sequential load (slower, less realistic).")
            for i in range(total_runs):
                city_code = int(random.choice(city_codes))
                payload = {"filters":{"city_code":city_code, "date_from":date_from, "date_to":date_to},"limit":10}
                j, dt = self.post("/search", payload)
                self.times["search"].append(dt)

    async def _search_httpx(self, client: "httpx.AsyncClient", payload: dict):
        try:
            j, dt = await post_json_httpx(client, self.base, "/search", payload)
            self.times["search"].append(dt)
            # light correctness check: ensure hits belong to requested city
            city_code = payload["filters"]["city_code"]
            for h in j.get("hits", []):
                if int(h.get("loc_id")) != int(city_code):
                    self.failures.append(f"load_test: hit city mismatch: got {h.get('loc_id')} expected {city_code}")
        except Exception as e:
            self.failures.append(f"load_test /search error: {e}")

    def _iso_to_sec1960(self, iso: str) -> int:
        y,m,d = [int(x) for x in iso.split("-")]
        return int((datetime(y,m,d,tzinfo=timezone.utc) - datetime(1960,1,1,tzinfo=timezone.utc)).total_seconds())

    def _assert_perf(self, key: str, threshold_ms: Optional[float]):
        if not threshold_ms:
            return
        arr = self.times.get(key, [])
        if not arr:
            return
        avg = sum(arr)/len(arr)
        p95 = statistics.quantiles(arr, n=20)[18] if len(arr) >= 20 else max(arr)
        if avg > threshold_ms:
            self.failures.append(f"perf: {key} avg {avg:.1f}ms > {threshold_ms:.1f}ms")
        self.notes.append(f"perf {key}: avg {avg:.1f} ms, p95 {p95:.1f} ms (n={len(arr)})")

    def _fmt_stats(self, key: str) -> str:
        arr = self.times.get(key, [])
        if not arr:
            return f"{key}: n=0"
        avg = sum(arr)/len(arr)
        p95 = statistics.quantiles(arr, n=20)[18] if len(arr) >= 20 else max(arr)
        return f"{key}: n={len(arr)} avg={avg:.1f}ms p95={p95:.1f}ms max={max(arr):.1f}ms"

    def print_summary(self):
        print("\n[SUMMARY]")
        for k in ["parse_date_range","lookup_city","lookup_company","search","answer"]:
            print("  " + self._fmt_stats(k))
        if self.notes:
            print("\n[NOTES]")
            for n in self.notes:
                print("  - " + n)
        if self.failures:
            print("\n[FAILURES]")
            for f in self.failures:
                print("  - " + f)
            print("\nRESULT: FAIL")
        else:
            print("\nRESULT: OK")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Comprehensive API self-test (perf + accuracy + bugs).")
    ap.add_argument("--base", default="http://localhost:8000", help="API base URL")
    ap.add_argument("--lookup", default="./lookup", help="Path to lookup directory (for mudurluk codes)")
    ap.add_argument("--cities", type=int, default=8, help="How many cities to sample")
    ap.add_argument("--per-city", type=int, default=3, help="Companies to harvest per city (from search results)")
    ap.add_argument("--search-runs", type=int, default=40, help="Total concurrent /search runs for load test")
    ap.add_argument("--concurrency", type=int, default=12, help="Concurrent level for httpx load test")
    ap.add_argument("--assert-search-ms", type=float, default=600.0, help="Assert avg /search latency (ms)")
    ap.add_argument("--assert-answer-ms", type=float, default=800.0, help="Assert avg /answer latency (ms)")
    args = ap.parse_args()

    if not HAVE_HTTPX and args.search_runs > 0 and args.concurrency > 1:
        print("[warn] httpx not installed; load test will run sequentially. Install with: pip install httpx")

    r = Runner(args.base, args.lookup)
    try:
        r.run(
            cities=args.cities,
            per_city=args.per_city,
            search_runs=args.search_runs,
            concurrency=args.concurrency,
            assert_search_ms=args.assert_search_ms,
            assert_answer_ms=args.assert_answer_ms,
        )
    except Exception as e:
        print("[FATAL]", e)
        print("RESULT: FAIL")

if __name__ == "__main__":
    main()

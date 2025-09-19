#!/usr/bin/env python3
import os, json, struct, mmap, sys, unicodedata
from datetime import date, timedelta

# Paths (same defaults as server)
ROOT = os.getcwd()
DATA_ROOT   = os.path.join(ROOT, "data")
LOOKUP_ROOT = os.path.join(ROOT, "lookup")
DOCMETA_BIN = os.path.join(DATA_ROOT, "docmeta", "docmeta.bin")

ROW = struct.Struct("<6i")   # [date_int, loc_id, type_id, comp_code, ad_id, ad_link_code]
EPOCH_1960 = date(1960, 1, 1)

# Turkish-aware normalization (matches your server)
TR = str.maketrans("ğĞüÜşŞıIİöÖçÇ","gGuUsSiIiOoCc")
def norm_tr(s:str)->str:
    if not s: return ""
    s = s.translate(TR).lower()
    s = unicodedata.normalize('NFD', s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def ymd_from_sec1960(sec:int)->str:
    return (EPOCH_1960 + timedelta(seconds=int(sec))).isoformat()

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        return default

def main():
    if not os.path.exists(DOCMETA_BIN):
        print(f"docmeta not found: {DOCMETA_BIN}", file=sys.stderr); sys.exit(1)

    # Args: company-substring (optional), year (optional)
    q = norm_tr(sys.argv[1]) if len(sys.argv) > 1 else None
    year = int(sys.argv[2]) if len(sys.argv) > 2 else None

    # Lookups (for pretty printing)
    unvan_vocab = load_json(os.path.join(LOOKUP_ROOT, "unvan_vocab.json"), {})
    mud_codes   = load_json(os.path.join(LOOKUP_ROOT, "mudurluk_codes.json"), {})
    type_codes  = load_json(os.path.join(LOOKUP_ROOT, "ilan_turu_codes.json"), {})

    # Reverse maps: code -> name
    mud_names = {}
    for name, code in mud_codes.items():
        try: mud_names[int(code)] = name
        except: pass
    type_names = {}
    for name, code in type_codes.items():
        try: type_names[int(code)] = name
        except: pass

    # Which company codes to include?
    if q:
        candidate_codes = { int(cid) for cid, nm in unvan_vocab.items() if q in norm_tr(nm) }
    else:
        candidate_codes = set(unvan_vocab.keys())  # everything (huge!)
        candidate_codes = { int(c) for c in candidate_codes }

    print(f"# candidates: {len(candidate_codes)}")
    if q:
        for cid in list(candidate_codes)[:20]:
            print(f"#  {cid}\t{unvan_vocab.get(str(cid))}")
        if len(candidate_codes) > 20:
            print(f"#  ... ({len(candidate_codes)-20} more)")

    # Scan docmeta.bin
    f = open(DOCMETA_BIN, "rb")
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    nrows = mm.size() // ROW.size
    print(f"# total rows: {nrows}")

    matches = 0
    # Print header (TSV)
    print("row_id\tdate\tcity\ttype\tcompany\tad_id")
    for rid in range(nrows):
        di, loc, typ, comp, adid, _ = ROW.unpack_from(mm, rid * ROW.size)

        if candidate_codes and (comp not in candidate_codes):
            continue

        d = EPOCH_1960 + timedelta(seconds=int(di))
        if year is not None and d.year != year:
            continue

        matches += 1
        if matches <= 1000:  # safety: stream first 1000 lines; adjust/remove if you truly want *all*
            print(f"{rid}\t{d.isoformat()}\t{mud_names.get(loc, loc)}\t{type_names.get(typ, typ)}\t{unvan_vocab.get(str(comp), comp)}\t{adid}")

    print(f"# matches: {matches}")
    mm.close(); f.close()

if __name__ == "__main__":
    main()

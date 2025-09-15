#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Static path configuration (override with environment variables).
"""

import os

ROOT         = os.path.abspath(os.getenv("PROJECT_ROOT", os.getcwd()))
DATA_ROOT    = os.path.abspath(os.getenv("DATA_ROOT",    os.path.join(ROOT, "data")))
LOOKUP_ROOT  = os.path.abspath(os.getenv("LOOKUP_ROOT",  os.path.join(ROOT, "lookup")))
INDEX_ROOT   = os.path.abspath(os.getenv("INDEX_ROOT",   os.path.join(DATA_ROOT, "index")))
SHARDS_ROOT  = os.path.abspath(os.getenv("SHARDS_ROOT",  os.path.join(DATA_ROOT, "index_sharded")))
DOCMETA_BIN  = os.path.abspath(os.getenv("DOCMETA_BIN",  os.path.join(DATA_ROOT, "docmeta", "docmeta.bin")))
DOCMETA_META = os.path.abspath(os.getenv("DOCMETA_META", os.path.join(DATA_ROOT, "docmeta", "meta.json")))

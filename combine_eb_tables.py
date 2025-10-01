#!/usr/bin/env python3
"""
Build a unified handbook data and merge to district-state and town info

1. Read all handbook_ebs csvs in PC01/district_handbooks/eb_table_extracts and combine
2. Merge with state-district crosswalk
"""

from ddlpy.utils import *


from pathlib import Path
import csv
import re
import os
import pandas as pd
import sys, shlex
import argparse

# ── base directory holding the handbook_ebs folders
p = argparse.ArgumentParser()
p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])

p.add_argument("--pdf_root", required=True)
p.add_argument("--hb_code", required=True)

# --- handle Stata passing a single-argument blob ---
argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])

args = p.parse_args(argv)


pdf_dir = Path(args.pdf_root)
code_dir = Path(args.hb_code)
                
BASE_DIR = pdf_dir / f"eb_table_extracts/"

out_hb  = BASE_DIR / "combined_hb" / "_file_manifest.csv"

# dist_xwalk_path = code_dir / f"{args.series}_hb_pdf_keys.csv"

## ---------------- cell: combined handbook data  ---------------- ##

hb_data = []

for filename in os.listdir(BASE_DIR):
    if not filename.endswith(".csv"):
        continue
    fp = BASE_DIR / filename

    try:
        # Peek header to get actual column names
        hdr = pd.read_csv(fp, nrows=0, engine="python")
        cols = list(hdr.columns)

        # Read CSV (forgiving on bad lines)
        df = pd.read_csv(fp, engine="python", on_bad_lines="skip")

    except Exception:
        # skip empty/malformed files quietly
        continue

    # If we got the expected 7 columns but no names, set them
    if df.shape[1] == 7 and set(df.columns) == set(range(7)):
        df.columns = ["location_code", "town_hb", "ward_name", "eb_no",
                      "total_pop", "sc_pop", "st_pop"]

    df = df.copy()
    df["source_file"] = fp.stem
    hb_data.append(df)

# Combine (guard if nothing valid)
if hb_data:
    hb_full = pd.concat(hb_data, ignore_index=True)
else:
    hb_full = pd.DataFrame(columns=["location_code","town_hb","ward_name","eb_no",
                                    "total_pop","sc_pop","st_pop","source_file"])
    

# merge with crosswalk
# dist_xwalk = pd.read_csv(dist_xwalk_path)

# dist_xwalk = (
#     dist_xwalk.assign(source_file=dist_xwalk["filename"].astype(str).str.removesuffix(".pdf"))
#               .drop(columns="filename")
#               .drop_duplicates(subset="source_file", keep="first")
# )

# no merge for now, write combined handbook directly to out_hb
# hb_merged = hb_full.merge(dist_xwalk, on="source_file", how="left", indicator=True)
# hb_merged = hb_merged.assign(town_abbr=hb_merged["town_hb"].astype(str).str[:4].str.lower())
hb_full.to_csv(out_hb,index=False)
# hb_merged.to_csv(out_hb, index=False)

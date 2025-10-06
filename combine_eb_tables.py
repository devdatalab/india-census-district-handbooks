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

dist_xwalk_path = code_dir / f"{args.series}_hb_pdf_keys.csv"

## ---------------- cell: combined handbook data  ---------------- ##

# Define the constants once
COLUMNS = ['location_code', 'town_hb', 'ward_name', 'eb_no',
           'total_pop', 'sc_pop', 'st_pop']
DTYPES = {
    0: 'string', 1: 'string', 2: 'string', 3: 'string',
    4: 'Int64', 5: 'Int64', 6: 'Int64'
}
EXPECTED_COL_COUNT = len(COLUMNS) # which is 7

# 1. Use a list comprehension to read and process all valid CSV files
hb_data_list = []
for filename in os.listdir(BASE_DIR):
    if filename.endswith('.csv'):
        filepath = os.path.join(BASE_DIR, filename)
        
        try:
            # Read the file with specified dtypes
            df = pd.read_csv(filepath, dtype=DTYPES)
            
            # Check for correct column count and non-empty data
            if df.shape[1] == EXPECTED_COL_COUNT and not df.empty:
                df.columns = COLUMNS
                df['source_file'] = filename.removesuffix(".csv")
                hb_data_list.append(df)
            else:
                 # print a message for skipped files
                 if df.shape[1] != EXPECTED_COL_COUNT:
                     print(f"Skipping {filename}: Incorrect column count ({df.shape[1]} != {EXPECTED_COL_COUNT})")
                 elif df.empty:
                     print(f"Skipping {filename}: Empty file or header only")

        except pd.errors.EmptyDataError:
            print(f"Skipping {filename}: File is empty or has no data to parse")
        except Exception as e:
             # Catch general errors like the IndexError from your previous run
            print(f"Error reading {filename}: {e}")

# 2. Combine the list of DataFrames into a single DataFrame
if hb_data_list:
    hb_full = pd.concat(hb_data_list, ignore_index=True)
    print(f"\nSuccessfully combined {len(hb_data_list)} files into one DataFrame with {len(hb_full)} rows.")
else:
    hb_full = pd.DataFrame() # Create an empty DataFrame if no files were processed
    print("No valid CSV files were processed and combined.")


# compare hb_full to the full list of pdfs in directory for coverage
hb_pdfs_indir = sorted([
    f for f in os.listdir(PC01)
    if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(PC01, f))
])

hb_pdfs_parsed = hb_full["source_file"].astype(str).tolist()
missing_inparsed = sorted(set(hb_pdfs_indir) - set(hb_pdfs_parsed))

## ---------------------------- cell: merge with state and district name  ---------------------------- ##

# Load crosswalk
dist_xwalk = pd.read_csv(dist_xwalk_path)

# remove .pdf suffix and create var that's filename (e.g., TEH_VOL-02_EB)
dist_xwalk = dist_xwalk.assign(source_file=dist_xwalk["filename"].str.removesuffix(".pdf")).drop(columns="filename")

# if filename doesn't end in _EB then add suffix
dist_xwalk.loc[~dist_xwalk["source_file"].str.lower().str.endswith("_eb", na=False), "source_file"] = dist_xwalk["source_file"] + "_EB"

dist_xwalk = dist_xwalk.drop_duplicates(subset = "source_file", keep = "first")

# Merge: hb_full = master, xwalk = using
hb_merged = hb_full.merge(dist_xwalk, on="source_file", how="left", indicator = True)

hb_merged = hb_merged.assign(
    town_abbr=hb_merged["town_hb"].str[:4].str.lower(),
)

hb_merged.to_csv(out_hb, index=False)

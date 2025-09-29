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

# ── base directory holding the handbook_ebs folders
BASE_DIR = PC01 / "district_handbooks/eb_table_extracts"
out_hb  = BASE_DIR / "combined_hb" / "_file_manifest.csv"

## ---------------- cell: combined handbook data  ---------------- ##

# XN: the only anormaly in structure is DH_16_2001_NTRI_EB.csv, everything else looks good
hb_data = []

for filename in os.listdir(BASE_DIR):
    if filename.endswith('.csv'):
        filepath = os.path.join(BASE_DIR, filename)
        df = pd.read_csv(filepath,dtype={
            0: 'string',
            1: 'string',
            2: 'string',
            3: 'string',
            4: 'Int64',
            5: 'Int64',
            6: 'Int64'
        })
        if df.shape[1] == 7:
            df.columns = ['location_code', 'town_hb', 'ward_name', 'eb_no',
                          'total_pop', 'sc_pop', 'st_pop']
            df = df.copy()
            df["source_file"] = filename.removesuffix(".csv")
            # print(df.head(4))
            hb_data.append(df)
             
# combine handbook data
hb_full = pd.concat(hb_data, ignore_index=True)

# compare hb_full to the full list of pdfs in directory for coverage
hb_pdfs_indir = sorted([
    f for f in os.listdir(PC01)
    if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(PC01, f))
])

hb_pdfs_parsed = hb_full["source_file"].astype(str).tolist()
missing_inparsed = sorted(set(hb_pdfs_indir) - set(hb_pdfs_parsed))

## ---------------------------- cell: merge with state and district name  ---------------------------- ##

# Merge with district-state crosswalk
dist_xwalk_path = "~/ddl/segregation/b/handbooks/hb_processing/pc01_hb_pdf_keys.csv"

# Load crosswalk
dist_xwalk = pd.read_csv(dist_xwalk_path)

# remove .pdf suffix and create var that's filename (e.g., TEH_VOL-02_EB)
dist_xwalk = dist_xwalk.assign(source_file=dist_xwalk["filename"].str.removesuffix(".pdf")).drop(columns="filename")

dist_xwalk = dist_xwalk.drop_duplicates(subset = "source_file", keep = "first")

# Merge: hb_full = master, xwalk = using
hb_merged = hb_full.merge(dist_xwalk, on="source_file", how="left", indicator = True)

hb_merged = hb_merged.assign(
    town_abbr=hb_merged["town_hb"].str[:4].str.lower(),
)

hb_merged.to_csv(out_hb, index=False)


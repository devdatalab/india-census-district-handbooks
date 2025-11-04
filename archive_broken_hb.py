#!/usr/bin/env python3
"""
archive_broken_handbooks.py

Moves all 'bad' or 'no_eb_tables' handbooks (problematic PDFs)
from:
    --pdf_root
    --pdf_root/taha_2025_09_19
into:
    --pdf_root/problematic_handbooks

Usage:
    python archive_broken_handbooks.py --series pc01 --pdf_root /path/to/handbooks
"""

import os
import sys
import argparse
import shlex
import shutil
from pathlib import Path

# ---------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------
p = argparse.ArgumentParser(description="Archive problematic district handbooks.")
p.add_argument("--series", required=True, choices=["pc01", "pc11", "pc91", "pc51"],
               help="Handbook series identifier (for consistency/logging only).")
p.add_argument("--pdf_root", required=True,
               help="Directory containing district handbook PDFs (e.g. /dartfs/.../district_handbooks).")

argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])
args = p.parse_args(argv)

root = Path(args.pdf_root).expanduser().resolve()
subdir = root / "taha_2025_09_19"
dest = root / "problematic_handbooks"
dest.mkdir(exist_ok=True)

# ---------------------------------------------------------------------
# Lists of problematic handbooks
# ---------------------------------------------------------------------
bad_pdfs = [
    "DH_02_2001_HAM","DH_02_2001_BIL","DH_02_2001_B_SHI",
    "DH_03_2001_RUP","DH_03_2001_FIR","DH_03_2001_PAT",
    "DH_05_2001_UTT","DH_05_2001_BAG",
    "DH_08_2001_ALW","DH_08_2001_DHA","DH_08_2001_DAU","DH_08_2001_JAI",
    "DH_08_2001_SIK","DH_08_2001_PAL","DH_08_2001_BUN","DH_08_2001_BHI",
    "DH_08_2001_RAJ","DH_08_2001_JHA",
    "DH_09_2001_JYO","DH_09_2001_GAU","DH_09_2001_AUR","DH_09_2001_JHA",
    "DH_09_2001_SUL_VOL-02","DH_09_2001_BAL",
    "DH_10_2001_SIW","DH_10_2001_SAR",
    "DH_14_2001_IMW","DH_14_2001_IME",
    "DH_19_2001_NTFP",
    "DH_20_2001_KOD","DH_20_2001_DEO","DH_20_2001_BOK",
    "DH_21_2001_SUN","DH_21_2001_MAY","DH_21_2001_DHE","DH_21_2001_KHO",
    "DH_21_2001_KOR",
    "DH_22_2001_KOR","DH_22_2001_MAH","DH_22_2001_BAS","DH_22_2001_DAN",
    "DH_27_2001_WAR","DH_27_2001_PUN","DH_27_2001_BID","DH_27_2001_OSM",
    "DH_27_2001_SOL","DH_27_2001_SIN","DH_27_2001_KOL","DH_27_2001_SAN",
    "DH_28_2001_PRA",
]

no_eb_tables = [
    "DH_02_2001_KIN", "DH_02_2001_LAH", "DH_09_2001_BLA_VOL-01", "DH_09_2001_DEO_VOL-02",
    "DH_09_2001_DEO_VOL-01", "DH_09_2001_GOD_VOL-01",
    "DH_09_2001_HAR_VOL-01", "DH_09_2001_KUS", "DH_09_2001_MAU_VOL-01",
    "DH_09_2001_MOR_VOL-01", "DH_09_2001_SHA_VOL-01","DH_14_2001_CHU",
    "DH_14_2001_SEN", "DH_14_2001_TAM", "DH_14_2001_UKH", "DH_15_2001_LAW",
    "DH_05_2001_TEH_VOL-01","DH_08_2001_CHI","DH_09_2001_ETA_VOL-01",
    "DH_09_2001_BUD_VOL-01","DH_12_2001_USIA","DH_14_2001_BIS",
    "DH_14_2001_IME","DH_14_2001_CHA","DH_17_2001_SGAR","DH_20_2001_PAL",
    "DH_20_2001_GUM","DH_21_2001_BAR","DH_21_2001_JHA","DH_21_2001_SAM",
    "DH_21_2001_DEB","DH_21_2001_KEN","DH_21_2001_BAL","DH_21_2001_BHA",
    "DH_21_2001_JAG","DH_21_2001_CUT","DH_21_2001_ANU","DH_21_2001_NAY",
    "DH_21_2001_PUR","DH_21_2001_GAN","DH_21_2001_KAN","DH_21_2001_BAU",
    "DH_21_2001_SON","DH_21_2001_BLN","DH_21_2001_KAL","DH_21_2001_RAY",
    "DH_21_2001_MAL","DH_24_2001_SUR","DH_29_2001_DKAN",
    "48295_2001_PUD","DH_09_2001_MOR_VOL-01","DH_09_2001_HAR_VOL-01",
    "DH_24_2001_DAN","DH_09_2001_SHA_VOL-01","DH_09_2001_MAU_VOL-01","DH_16_2001_STRI",
    "DH_09_2001_BLA_VOL-01"
]

pdf_duplicates = ["Village and Towwise Primary Census Abstract Mahendragarh, Part XII-A & B, Series-7, Haryana"]

problematic = set(bad_pdfs + no_eb_tables + pdf_duplicates)

# ---------------------------------------------------------------------
# Archive logic
# ---------------------------------------------------------------------
found, missing = [], []

for stem in problematic:
    pattern = f"{stem}.pdf"
    matches = list(root.glob(pattern)) + list(subdir.glob(pattern))
    if not matches:
        missing.append(stem)
        continue

    for pdf in matches:
        dest_path = dest / pdf.name
        shutil.move(str(pdf), dest_path)
        found.append(pdf.name)

# ---------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------
print(f"[OK] Archived {len(found)} problematic handbooks for series {args.series} to:")
print(f"     {dest}")
if missing:
    print(f"[WARN] {len(missing)} not found:")
    for m in missing:
        print("  ", m)

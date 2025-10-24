import pandas as pd
import os
import argparse
import sys, shlex
from PyPDF2 import PdfReader, PdfWriter
from pathlib import Path

def find_longest_consecutive_sequence(pages):
    pages = sorted(set(int(p) for p in pages if pd.notna(p)))
    if not pages:
        return []
    longest, current = [], [pages[0]]
    for i in range(1, len(pages)):
        if pages[i] == pages[i-1] + 1:
            current.append(pages[i])
        else:
            if len(current) > len(longest):
                longest = current
            current = [pages[i]]
    if len(current) > len(longest):
        longest = current
    return longest

def load_pages_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    try:
        df = pd.read_csv(csv_path)
        cols = [c.strip().lower() for c in df.columns]
        if "filename" in cols and "page_number" in cols:
            df.columns = ["filename", "page_number"]
            return df[["filename", "page_number"]]
    except Exception:
        pass
    return pd.read_csv(csv_path, header=None, names=["filename", "page_number"])

# --- CLI ---
p = argparse.ArgumentParser(
    description="Cut EB page ranges from PDFs using urban_eb_pages.csv as a guide."
)
p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
p.add_argument("--pdf_root", required=True,
               help="Folder containing urban_eb_pages.csv and eb_table_extracts/.")
p.add_argument("--pdf_source_directory", default="",
               help="Subfolder (under --pdf_root) where the input PDFs live. "
                    "If empty, PDFs are read directly from --pdf_root.")
# Handle Stata single-argument blob
argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])
args = p.parse_args(argv)

# Paths
pdf_root = Path(args.pdf_root)
csv_path = pdf_root / "urban_eb_pages.csv"

sub = (args.pdf_source_directory or "").strip()
pdf_source_dir = (pdf_root / sub) if sub else pdf_root
output_dir = pdf_root / "eb_table_extracts"
output_dir.mkdir(parents=True, exist_ok=True)

# Load CSV (unchanged)
df = load_pages_csv(csv_path)

# Extract per file
for filename, group in df.groupby("filename", sort=False):
    pages = (
        pd.to_numeric(group["page_number"], errors="coerce")
          .dropna()
          .astype(int)
          .tolist()
    )
    if not pages:
        print(f"{filename}: no numeric pages in CSV → skip")
        continue

    best_seq = find_longest_consecutive_sequence(pages)
    if not best_seq:
        print(f"{filename}: no consecutive page sequence found → skip")
        continue

    start_0 = best_seq[0] - 1
    end_0   = best_seq[-1] - 1

    input_pdf_path = (pdf_source_dir / filename).resolve()
    if not input_pdf_path.exists():
        print(f"{filename}: not found in {pdf_source_dir} → skip")
        continue

    output_pdf_path = output_dir / filename.replace(".pdf", "_EB.pdf")

    try:
        reader = PdfReader(str(input_pdf_path))
        n_pages = len(reader.pages)
        if start_0 < 0 or end_0 >= n_pages or start_0 > end_0:
            print(f"{filename}: invalid page range {start_0+1}-{end_0+1} of {n_pages} → skip")
            continue

        writer = PdfWriter()
        for i in range(start_0, end_0 + 1):
            writer.add_page(reader.pages[i])

        with open(output_pdf_path, "wb") as f_out:
            writer.write(f_out)

        print(f"Extracted {filename}: pages {start_0+1}-{end_0+1} → {output_pdf_path}")
    except Exception as e:
        print(f"Failed to process {filename}: {type(e).__name__}: {e}")

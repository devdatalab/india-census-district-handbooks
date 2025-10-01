import pandas as pd
import os
import argparse
import sys, shlex
from PyPDF2 import PdfReader, PdfWriter
from pathlib import Path

def find_longest_consecutive_sequence(pages):
    pages = sorted(set(pages))
    longest = []
    current = [pages[0]]

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


# get paths
p = argparse.ArgumentParser()
p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])

# folder containing csv to scan
p.add_argument("--pdf_root", required=True)

# --- handle Stata passing a single-argument blob ---
argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])

args = p.parse_args(argv)


pdf_dir = Path(args.pdf_root)
csv_path = pdf_dir / f"urban_eb_pages.csv" 
output_dir = pdf_dir / f"eb_table_extracts/"

# Load CSV
df = pd.read_csv(csv_path, header=None, names=["filename", "page_number"])

# Group by filename
for filename, group in df.groupby("filename"):
    # Coerce to numeric, drop non-numeric, cast to int, then to list
    pages = (
        pd.to_numeric(group["page_number"], errors="coerce")
          .dropna()
          .astype(int)
          .tolist()
    )
    if not pages:
        continue
    best_seq = find_longest_consecutive_sequence(pages)
    if not best_seq:
        print(f"No valid page sequence found for {filename}")
        continue

    start = best_seq[0] - 1  # 0-indexed
    end = best_seq[-1] - 1

    input_pdf_path = os.path.join(pdf_dir, filename)
    output_pdf_path = os.path.join(output_dir, filename.replace(".pdf", "_EB.pdf"))

    try:
        reader = PdfReader(input_pdf_path)
        writer = PdfWriter()

        for i in range(start, end + 1):
            writer.add_page(reader.pages[i])

        with open(output_pdf_path, "wb") as f_out:
            writer.write(f_out)
        print(f"Extracted {filename}: pages {start+1}-{end+1}")
    except Exception as e:
        print(f"Failed to process v{filename}: {e}")

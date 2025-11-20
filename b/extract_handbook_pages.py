import pandas as pd
import os
import argparse
import sys, shlex
from PyPDF2 import PdfReader, PdfWriter
from pathlib import Path

def load_page_ranges_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    
    required_cols = {'filename', 'start_page', 'end_page'}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(f"CSV must have columns: filename, start_page, end_page")
    
    return df

# --- CLI ---
p = argparse.ArgumentParser(
    description="Cut EB page ranges from PDFs using page_ranges_for_review.csv as a guide."
)
p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
p.add_argument("--pdf_root", required=True,
               help="Folder containing page_ranges_for_review.csv and eb_table_extracts/.")
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
csv_path = pdf_root / f"{args.series}_page_ranges_for_review.csv"
sub = (args.pdf_source_directory or "").strip()
pdf_source_dir = (pdf_root / sub) if sub else pdf_root
output_dir = pdf_root / "eb_table_extracts"
output_dir.mkdir(parents=True, exist_ok=True)

# Load CSV
print(f"Loading page ranges from: {csv_path}")
df = load_page_ranges_csv(csv_path)

# Filter to only rows with valid page ranges
df = df.dropna(subset=['start_page', 'end_page'])
df['start_page'] = df['start_page'].astype(int)
df['end_page'] = df['end_page'].astype(int)

print(f"Found {len(df)} files to process\n")

# Extract per file
results = []
skipped = []

for idx, row in df.iterrows():
    filename = row['filename']
    start_page = row['start_page']
    end_page = row['end_page']
    
    # Convert to 0-indexed
    start_0 = start_page - 1
    end_0 = end_page - 1
    
    input_pdf_path = (pdf_source_dir / filename).resolve()
    
    if not input_pdf_path.exists():
        print(f"{filename}: not found in {pdf_source_dir} → skip")
        skipped.append(filename)
        continue
    
    output_pdf_path = output_dir / filename.replace(".pdf", "_EB.pdf")
    
    try:
        reader = PdfReader(str(input_pdf_path))
        n_pages = len(reader.pages)
        
        if start_0 < 0 or end_0 >= n_pages or start_0 > end_0:
            print(f"{filename}: invalid page range {start_page}-{end_page} (PDF has {n_pages} pages) → skip")
            skipped.append(filename)
            continue
        
        writer = PdfWriter()
        for i in range(start_0, end_0 + 1):
            writer.add_page(reader.pages[i])
        
        with open(output_pdf_path, "wb") as f_out:
            writer.write(f_out)
        
        print(f"✓ {filename}: extracted pages {start_page}-{end_page} → {output_pdf_path.name}")
        
        results.append({
            'filename': filename,
            'start_page': start_page,
            'end_page': end_page,
            'num_pages': end_page - start_page + 1,
            'output_file': output_pdf_path.name,
            'status': 'success'
        })
        
    except Exception as e:
        print(f"✗ {filename}: {type(e).__name__}: {e}")
        skipped.append(filename)

# Save extraction summary
if results:
    results_df = pd.DataFrame(results)
    summary_csv = output_dir / f"{args.series}_extraction_summary.csv"
    results_df.to_csv(summary_csv, index=False)
    print(f"\n✓ Extraction summary saved to {summary_csv}")

print(f"\n=== Summary ===")
print(f"Successfully extracted: {len(results)}")
print(f"Skipped/Failed: {len(skipped)}")


#!/usr/bin/env python3
"""
find_pages.py

Scans every PDF in:

    /dartfs/rc/lab/I/IEC/pc01/district_handbooks

for either of the (spacing-agnostic) phrases:

    "APPENDIX TO DISTRICT PRIMARY"
    "URBAN BLOCK WISE"
    "TOTAL, SCHEDULED CASTES AND SCHEDULED TRIBES POPULATION - URBAN BLOCK WISE"

Every matching page is streamed to

    ~/iec/pc01/district_handbooks/urban_eb_pages.csv

as:

    filename,page_number

The CSV is flushed and fsync’d continuously so you can watch progress
with `tail -f`.  The script first tries the lenient parser in `pypdf`;
if that fails on a malformed file it falls back to the more tolerant
`PyMuPDF` parser.

Dependencies (install/upgrade in your conda env):
    pip install --upgrade pypdf "pymupdf>=1.24" tqdm

Note: this is GPT-o3 code, untouched other than some output verification.
"""

# ---------------------------------------------------------------------
#  Standard-library imports
# ---------------------------------------------------------------------

# Provides filesystem-path objects with helpful methods
from pathlib import Path

# Regular expressions for whitespace-agnostic phrase matching
import re

# CSV writing utilities
import csv

# Access to low-level OS calls (fsync)
import os

# Exit cleanly on user interruption
import sys

import argparse

from ddlpy.utils import * 

# ---------------------------------------------------------------------
#  Third-party imports (loaded lazily where practical)
# ---------------------------------------------------------------------

# tqdm is only used for progress bars; import later to avoid startup cost
from contextlib import suppress   # Used for graceful KeyboardInterrupt handling

# ---------------------------------------------------------------------
#  Configuration constants
# ---------------------------------------------------------------------
import sys, shlex

p = argparse.ArgumentParser()
p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])

# folder containing csv to scan
p.add_argument("--pdf_root", required=True)

# --- handle Stata passing a single-argument blob ---
argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])

args = p.parse_args(argv)


PDF_ROOT = Path(args.pdf_root)
OUT_CSV = PDF_ROOT / f"urban_eb_pages.csv" 

# Target phrases as they appear in the documents
PHRASES = [
    "APPENDIX TO DISTRICT PRIMARY",
    "URBAN BLOCK WISE",
    "TOTAL, SCHEDULED CASTES AND SCHEDULED TRIBES POPULATION",
]

EB_COLUMN_HINTS = [
    "LOCATION CODE", "NAME OF TOWN", "NAME OF WARD", 
]

# ---------------------------------------------------------------------
#  Helper functions
# ---------------------------------------------------------------------
# Check if already have a path name in file and skip it if so
# Comment out if trying to run again from the beginning 
def _load_already_processed(out_csv_path):
    """
    Load the set of filenames already written to the CSV so we can skip them.
    """
    if not Path(out_csv_path).exists():
        return set()

    with open(out_csv_path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        return set(row[0] for row in reader) 


# Normalise a string: remove all whitespace and force upper-case
def _normalise(txt: str) -> str:
    """
    Collapse whitespace and capitalise so that
    line breaks / multiple spaces are ignored.
    """
    return re.sub(r"\s+", "", txt.upper())


# Pre-normalise the target phrases once so we don’t repeat work per page
PHRASES_NORM = [_normalise(p) for p in PHRASES]


# Convert a list of page numbers into a compact human-readable string
def _pages_to_ranges(pages):
    """
    [3,4,5,9,11,12] -> '3-5, 9, 11-12'
    """
    # itertools imported inline to keep global namespace tidy
    import itertools

    # List that will accumulate range strings
    ranges = []

    # Group consecutive pages together
    for _, group in itertools.groupby(enumerate(pages), lambda t: t[0] - t[1]):
        seq = [x for _, x in group]
        # Use 'start-end' for ranges, single number otherwise
        ranges.append(f"{seq[0]}-{seq[-1]}" if len(seq) > 1 else f"{seq[0]}")

    # Comma-separated string ready for printing
    return ", ".join(ranges)


# ---------------------------------------------------------------------
#  PDF-scanning back-ends
# ---------------------------------------------------------------------

# Attempt to scan a PDF with pypdf; return list of hits or None on failure
def _scan_with_pypdf(path, pbar):
    """
    Primary parser: pypdf (strict=False) tolerates many malformed PDFs.
    """
    # Import locally to avoid cost when module is absent
    from pypdf import PdfReader, errors

    try:
        # strict=False disables many validity checks
        reader = PdfReader(path, strict=False)
        hits = []

        # Enumerate pages starting at 1
        for pno, page in enumerate(reader.pages, 1):
            text = page.extract_text() or ""
            if (any(ph in _normalise(text) for ph in PHRASES_NORM) and
                any(hint in text.upper() for hint in EB_COLUMN_HINTS)
            ):
                hits.append(pno) 
            pbar.update(1)    # Tick the progress bar

        return hits

    # If pypdf chokes, return None so caller knows to fall back
    #except errors.PdfReadError:
    except Exception as e:
        print(f"pypdf failed on {path.name} with error: {type(e).__name__}: {e}") 
        return None
    


# Fallback parser: PyMuPDF (fitz) – slower but extremely tolerant
def _scan_with_pymupdf(path, pbar):
    """
    Secondary parser: handles badly broken PDFs that pypdf rejects.
    """
    import fitz  # PyMuPDF

    hits = []

    # Open the document; context manager auto-closes
    with fitz.open(path) as doc:
        for pno in range(doc.page_count):
            text = doc.load_page(pno).get_text("text")
            if (any(ph in _normalise(text) for ph in PHRASES_NORM) and
                any(hint in text.upper() for hint in EB_COLUMN_HINTS)
            ):
                hits.append(pno + 1)   # PyMuPDF is 0-based; convert to 1-based
            pbar.update(1)

    return hits


# Wrapper that chooses the right parser and returns page numbers
def _scan_pdf(path):
    """
    Try pypdf first; if it returns None, retry with PyMuPDF.
    Shows a per-file tqdm bar that ticks once per page scanned.
    """
    # Need total pages to size the progress bar
    try:
        from pypdf import PdfReader
        n_pages = len(PdfReader(path, strict=False).pages)
    except Exception:
        import fitz
        n_pages = fitz.open(path).page_count

    # Lazy import tqdm only when needed
    from tqdm import tqdm

    # Create the progress bar (leave=False keeps console clean)
    with tqdm(
        total=n_pages,
        desc=f"processing {path.name}",
        unit="pg",
        ncols=80,
        leave=False,
    ) as pbar:

        # First attempt with pypdf
        pages = _scan_with_pypdf(path, pbar)

        # If pypdf failed, reset bar and retry with PyMuPDF
        if pages is None:
            pbar.reset()
            pages = _scan_with_pymupdf(path, pbar)

    return pages


# ---------------------------------------------------------------------
#  Main execution routine
# ---------------------------------------------------------------------

def main():
    """
    Walk PDF_ROOT, scan each PDF, stream results to OUT_CSV,
    and print a summary line per file.
    """
    # Ensure output directory exists
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Detect whether we need to write the CSV header
    first_run = not OUT_CSV.exists()

    error_files = ["DH_19_2001_NTFP.pdf", ]
    
    # Open CSV in append mode, line-buffered (buffering=1)
    with OUT_CSV.open("a", newline="", buffering=1) as fh:
        writer = csv.writer(fh)

        # Write header once if file is new
        if first_run:
            writer.writerow(["filename", "page_number"])
            fh.flush()
            os.fsync(fh.fileno())

        # Iterate over PDFs in deterministic order
        already_processed = _load_already_processed(OUT_CSV)
        all_pdfs = sorted(PDF_ROOT.glob("*.pdf"))

        # --- ADDED: simple counters for a summary ---
        total = len(all_pdfs)
        scanned_ok = 0
        with_hits = 0
        no_hits = 0
        failed = 0
        rows_written = 0
        # -------------------------------------------

        for pdf in all_pdfs:
            
            if pdf.name in error_files:
                print(f"{pdf.name}: Getting error for this file")
                continue
             
            # Include if not making any changes to how pages parsed 
            # and don't want to research papers have pages for
            """ 
            if pdf.name in already_processed:
                print(f"{pdf.name}: Skipping already-processed file")
                continue
            """
            
            if not pdf.exists(): 
                print(f"{pdf.name}: Skipping missing file") 
                continue 
            if not os.access(pdf, os.R_OK): 
                print(f"{pdf.name}: Skipping unreadable file") 
                continue 

            # Allow user to Ctrl-C without ugly traceback
            try:
                pages = _scan_pdf(pdf)
                scanned_ok += 1  # ADDED: parsed successfully

                if pages:
                    for p in pages:
                        writer.writerow([pdf.name, p])
                        fh.flush()        # Make row visible immediately
                        rows_written += 1 # ADDED: count output rows
                    os.fsync(fh.fileno()) # Force data to disk after each file
                    with_hits += 1        # ADDED: pdfs with at least one hit
                    print(f"{pdf.name}: {_pages_to_ranges(pages)}")
                else:
                    no_hits += 1          # ADDED: pdfs with zero hits
                    print(f"{pdf.name}: no hits")

            # Clean exit on interruption
            except KeyboardInterrupt:
                sys.exit("\nInterrupted by user")
            except Exception as e:
                failed += 1               # ADDED: pdfs that raised errors
                print(f"{pdf.name}: ERROR {type(e).__name__}: {e}")

    # --- ADDED: end-of-run summary ---
    print("\n[SUMMARY]")
    print(f"  PDFs total:        {total}")
    print(f"  Parsed OK:         {scanned_ok}/{total}")
    print(f"  With hits:         {with_hits} (rows written: {rows_written})")
    print(f"  No hits:           {no_hits}")
    print(f"  Failed to parse:   {failed}")
    print(f"  Output CSV:        {OUT_CSV} ({'created' if first_run else 'appended'})")
    # ---------------------------------


# ---------------------------------------------------------------------
#  Entry-point guard (so the file can be imported without side effects)
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
find_pages.py

Scans every PDF in a given directory for pages that likely contain urban EB tables,
using whitespace-insensitive phrase matching. Results stream to a CSV so you can
monitor progress with `tail -f`.

Key features:
  - Skip-by-default: if a PDF already has rows in the output CSV, it is not re-scanned.
  - Reprocess option: `--reprocess 1` recreates the CSV and re-scans all PDFs.

Examples:
  python find_pages.py --series pc01 --pdf_root /dartfs/rc/lab/I/IEC/pc01/district_handbooks
  python find_pages.py --series pc01 --pdf_root /dartfs/rc/lab/I/IEC/pc01/district_handbooks --reprocess 1

Dependencies (install/upgrade in your conda env):
  pip install --upgrade pypdf "pymupdf>=1.24" tqdm
"""

# ---------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------
from pathlib import Path
import re
import csv
import os
import sys
import shlex
import argparse

# ---------------------------------------------------------------------
# Optional project utils (left as-is from your original)
# ---------------------------------------------------------------------
from ddlpy.utils import *  # noqa: F401,F403

# ---------------------------------------------------------------------
# Third-party (imported lazily where sensible)
# ---------------------------------------------------------------------
from contextlib import suppress  # noqa: F401  (kept for parity with original)

# ---------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------
p = argparse.ArgumentParser(description="Scan PDFs for urban EB pages and stream results to CSV.")
p.add_argument("--series", required=True, choices=["pc01", "pc11", "pc91", "pc51"],
               help="Handbook series identifier (affects only validation/logging here).")
p.add_argument("--pdf_root", required=True,
               help="Directory containing the PDFs. Output CSV will be written here as 'urban_eb_pages.csv'.")
p.add_argument("--reprocess", type=int, choices=[0, 1], default=0,
               help="0=default: skip files already present in CSV; 1=recreate CSV and re-scan all PDFs.")

# Handle Stata passing a single-argument blob (e.g., one long quoted string).
argv = sys.argv[1:]
if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
    argv = shlex.split(argv[0])
args = p.parse_args(argv)

# Paths
PDF_ROOT = Path(args.pdf_root)
OUT_CSV = PDF_ROOT / "urban_eb_pages.csv"

# ---------------------------------------------------------------------
# Detection phrases / hints
#   - PHRASES are matched in a whitespace-insensitive way (normalized).
#   - EB_COLUMN_HINTS are matched as plain uppercase substrings.
# ---------------------------------------------------------------------
PHRASES = [
    "APPENDIX TO DISTRICT PRIMARY",
    "URBAN BLOCK WISE",
    "TOTAL, SCHEDULED CASTES AND SCHEDULED TRIBES POPULATION",
]
EB_COLUMN_HINTS = ["LOCATION CODE", "NAME OF TOWN", "NAME OF WARD"]


def _normalise(txt: str) -> str:
    """Remove all whitespace and uppercase for whitespace-insensitive matching."""
    return re.sub(r"\s+", "", txt.upper())


PHRASES_NORM = [_normalise(p) for p in PHRASES]


def _pages_to_ranges(pages):
    """
    Convert [3,4,5,9,11,12] -> '3-5, 9, 11-12' for nicer logging.
    """
    import itertools
    ranges = []
    for _, group in itertools.groupby(enumerate(pages), lambda t: t[0] - t[1]):
        seq = [x for _, x in group]
        ranges.append(f"{seq[0]}-{seq[-1]}" if len(seq) > 1 else f"{seq[0]}")
    return ", ".join(ranges)


# ---------------------------------------------------------------------
# CSV helpers (skip-by-default support)
# ---------------------------------------------------------------------
def _load_already_processed_filenames(out_csv_path: Path) -> set[str]:
    """
    Return a set of filenames that already have at least one row in OUT_CSV.
    We only need filenames (not pages) to decide whether to skip a file.
    """
    if not out_csv_path.exists():
        return set()
    done = set()
    with out_csv_path.open(newline="") as f:
        r = csv.reader(f)
        header = next(r, None)
        for row in r:
            if not row:
                continue
            # row = [filename, page_number]
            done.add(row[0])
    return done


# ---------------------------------------------------------------------
# PDF scanners
# ---------------------------------------------------------------------
def _scan_with_pypdf(path, pbar):
    """
    Primary parser using pypdf (strict=False) – fast and works for many PDFs.
    Returns a list of 1-based page numbers containing matches, or None on failure.
    """
    from pypdf import PdfReader
    try:
        reader = PdfReader(path, strict=False)
        hits = []
        for pno, page in enumerate(reader.pages, 1):
            text = page.extract_text() or ""
            # Match if any target phrase (normalized) appears AND any EB column hint is visible.
            if (any(ph in _normalise(text) for ph in PHRASES_NORM)
                    and any(h in text.upper() for h in EB_COLUMN_HINTS)):
                hits.append(pno)
            pbar.update(1)
        return hits
    except Exception as e:
        print(f"pypdf failed on {path.name} with error: {type(e).__name__}: {e}")
        return None


def _scan_with_pymupdf(path, pbar):
    """
    Fallback parser using PyMuPDF (fitz) – slower but very tolerant.
    Returns a list of 1-based page numbers containing matches.
    """
    import fitz
    hits = []
    with fitz.open(path) as doc:
        for pno in range(doc.page_count):
            text = doc.load_page(pno).get_text("text")
            if (any(ph in _normalise(text) for ph in PHRASES_NORM)
                    and any(h in text.upper() for h in EB_COLUMN_HINTS)):
                hits.append(pno + 1)
            pbar.update(1)
    return hits


def _scan_pdf(path: Path) -> list[int]:
    """
    Determine number of pages (for progress bar), try pypdf first, then fall back to PyMuPDF.
    Return a list of 1-based page numbers with hits (possibly empty list).
    """
    # Determine page count for progress bar
    try:
        from pypdf import PdfReader
        n_pages = len(PdfReader(path, strict=False).pages)
    except Exception:
        import fitz
        n_pages = fitz.open(path).page_count

    # Progress bar scoped to this file
    from tqdm import tqdm
    with tqdm(total=n_pages,
              desc=f"processing {path.name}",
              unit="pg", ncols=80, leave=False) as pbar:

        pages = _scan_with_pypdf(path, pbar)
        if pages is None:
            pbar.reset()
            pages = _scan_with_pymupdf(path, pbar)

    return pages


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    """
    Walk PDF_ROOT, scan each PDF, and stream results to OUT_CSV as (filename, page_number).
    - If --reprocess 0 (default): append mode; skip files already present in CSV.
    - If --reprocess 1: recreate CSV and re-scan everything.
    """
    # Ensure output directory exists
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Known-bad PDFs to skip entirely (customize as needed)
    error_files = {"DH_19_2001_NTFP.pdf"}

    # Choose mode based on reprocess
    recreate = (args.reprocess == 1)
    mode = "w" if recreate else "a"
    first_run = True if recreate else (not OUT_CSV.exists())

    # Open output and write header if needed
    with OUT_CSV.open(mode, newline="", buffering=1) as fh:
        writer = csv.writer(fh)
        if first_run:
            writer.writerow(["filename", "page_number"])
            fh.flush()
            os.fsync(fh.fileno())

        # Build skip set unless we are recreating from scratch
        already_processed = set() if recreate else _load_already_processed_filenames(OUT_CSV)

        # Mode banner
        if recreate:
            print("[MODE] reprocess=1 → recreating CSV and re-scanning all PDFs.")
        else:
            print(f"[MODE] reprocess=0 → skipping {len(already_processed)} PDFs already present in CSV.")

        # Gather PDFs in deterministic order
        all_pdfs = sorted(PDF_ROOT.glob("*.pdf"))

        # Counters
        total = len(all_pdfs)
        scanned_ok = 0
        with_hits = 0
        no_hits = 0
        failed = 0
        rows_written = 0
        skipped = 0

        for pdf in all_pdfs:
            # Quick guards
            if pdf.name in error_files:
                print(f"{pdf.name}: Skipping (known error)")
                continue
            if not pdf.exists():
                print(f"{pdf.name}: Skipping missing file")
                continue
            if not os.access(pdf, os.R_OK):
                print(f"{pdf.name}: Skipping unreadable file")
                continue
            if not recreate and pdf.name in already_processed:
                skipped += 1
                print(f"{pdf.name}: already in CSV → skip")
                continue

            # Scan and stream rows
            try:
                pages = _scan_pdf(pdf)
                scanned_ok += 1
                if pages:
                    for p in pages:
                        writer.writerow([pdf.name, p])
                        fh.flush()          # show progress immediately
                        rows_written += 1
                    os.fsync(fh.fileno())   # ensure durability per file
                    with_hits += 1
                    print(f"{pdf.name}: {_pages_to_ranges(pages)}")
                else:
                    no_hits += 1
                    print(f"{pdf.name}: no hits")
            except KeyboardInterrupt:
                sys.exit("\nInterrupted by user")
            except Exception as e:
                failed += 1
                print(f"{pdf.name}: ERROR {type(e).__name__}: {e}")

    # Summary
    print("\n[SUMMARY]")
    print(f"  PDFs total:         {total}")
    print(f"  Parsed OK:          {scanned_ok}/{total}")
    print(f"  Skipped (existing): {skipped}")
    print(f"  With hits:          {with_hits} (rows written this run: {rows_written})")
    print(f"  No hits:            {no_hits}")
    print(f"  Failed to parse:    {failed}")
    print(f"  Output CSV:         {OUT_CSV} ({'created' if (recreate or first_run) else 'appended'})")


# Entry point
if __name__ == "__main__":
    main()

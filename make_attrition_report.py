#!/usr/bin/env python3
"""
hb_loss_report.py

Generate a Markdown report for handbook processing attrition.

Outputs
-------
1) Overall attrition funnel table:
     Stage | Kept | % of Total | Dropped from Prev
2) Drill-down tables:
     - Missing PDFs
     - No EB pages (given PDFs)
     - No CSV (given EB pages)
     - No reliable EB rows (given CSV)

Input (derived from --series & --in_dir)
----------------------------------------
  <in_dir>/<series>_handbook_processing_loss.dta
    Required columns:
      <series>_state_id, <series>_state_name,
      <series>_district_id, <series>_district_name,
      has_pdf, has_eb_pages, llm_csv, has_eb_rows
    Optional: filename (if present, included in drill-downs)

Output (to --out_dir)
---------------------
  <out_dir>/<series>_hb_processing_report.md
"""

import sys, shlex
import argparse
from pathlib import Path
import pandas as pd

# Stages must match your Stata pipeline (Stage 1..4)
STAGES = ["has_pdf", "has_eb_pages", "llm_csv", "has_eb_rows"]
LABELS = ["PDF present", "EB pages found", "CSV extracted", "Reliable EB rows"]

def ensure_binary(df: pd.DataFrame, cols) -> None:
    """
    Coerce stage columns to {0,1}:
    - Treat missing/NaN as 0
    - Any positive numeric/parsable string as 1
    """
    for c in cols:
        # Convert strings like "0"/"1" safely; leave NaN -> 0
        s = pd.to_numeric(df[c], errors="coerce").fillna(0)
        df[c] = (s > 0).astype("int8")

def funnel_keeps(df: pd.DataFrame, stages):
    """
    Compute cumulative keeps at each stage:
      kept[i] = number of rows with all stages[0..i] == 1
    """
    total = len(df)
    keeps = []
    kept = df
    for i, c in enumerate(stages):
        kept = kept[kept[c] == 1] if i else df[df[c] == 1]
        keeps.append(len(kept))
    return total, keeps

def fmt_pct(n: int, d: int) -> str:
    return "0.00%" if d == 0 else f"{(100.0 * n / d):.2f}%"

def main():
    # -------- args --------
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--in_dir",  default=".", help="Dir with <series>_handbook_processing_loss.dta")
    ap.add_argument("--out_dir", default=".", help="Dir to write <series>_hb_processing_report.md")
    ap.add_argument("--title",   default=None, help="Optional report title override")

    # Handle Stata/Make-style single-blob argv
    argv = sys.argv[1:]
    if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
        argv = shlex.split(argv[0])
    args = ap.parse_args(argv)

    series = args.series
    in_path  = Path(args.in_dir).expanduser().resolve()  / f"{series}_handbook_processing_loss.dta"
    out_path = Path(args.out_dir).expanduser().resolve() / f"{series}_hb_processing_report.md"
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # -------- load --------
    df = pd.read_stata(in_path)

    # Required ID / name columns (as emitted by Stata scripts)
    s_id, s_nm = f"{series}_state_id",    f"{series}_state_name"
    d_id, d_nm = f"{series}_district_id", f"{series}_district_name"
    fncol = "filename" if "filename" in df.columns else None

    for col in [s_id, s_nm, d_id, d_nm, *STAGES]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Normalize stage columns to strict {0,1}
    ensure_binary(df, STAGES)

    # -------- overall funnel --------
    total, keeps = funnel_keeps(df, STAGES)

    lines = []
    title = args.title or f"{series.upper()} Handbook Processing — Markdown Report"
    lines.append(f"# {title}\n")

    lines.append("## Overall Attrition Funnel\n")
    lines.append("| Stage | Kept | % of Total | Dropped from Prev |")
    lines.append("|---|---:|---:|---:|")
    prev_kept = total
    for i, label in enumerate(LABELS):
        kept = keeps[i]
        dropped = prev_kept - kept
        lines.append(f"| {i+1}. {label} | {kept} | {fmt_pct(kept, total)} | {dropped} |")
        prev_kept = kept
    lines.append("")

    # -------- drill-downs --------
    def block(title: str, frame: pd.DataFrame):
        lines.append(f"## {title}\n")
        if frame.empty:
            lines.append("_None_\n")
            return
        cols = [s_id, s_nm, d_id, d_nm] + ([fncol] if fncol else [])
        # Header
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        # Rows (sorted by IDs; keep string order to respect zero-padding)
        for _, r in frame.sort_values([s_id, d_id]).iterrows():
            vals = [str(r.get(c, "")) if pd.notna(r.get(c, "")) else "" for c in cols]
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

    # Stage-specific subsets (respecting the funnel ordering)
    missing_pdf = df[df["has_pdf"].eq(0)]
    no_eb_pages = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(0)]
    no_csv      = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(0)]
    no_rows     = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(1) & df["has_eb_rows"].eq(0)]

    block("Missing PDFs", missing_pdf)
    block("No EB pages (given PDFs)", no_eb_pages)
    block("No CSV (given EB pages)", no_csv)
    block("No reliable EB rows (given CSV)", no_rows)

    # -------- write --------
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()

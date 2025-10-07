#!/usr/bin/env python3
"""
hb_loss_report.py
Generate a Markdown report for handbook processing.

Outputs:
  - Overall attrition funnel table:
      Stage | Kept | % of Total | Dropped from Prev
  - Drill-down tables:
      Missing PDFs
      No EB pages (given PDFs)
      No CSV (given EB pages)
      No reliable EB rows (given CSV)

Input (derived from --series & --in_dir):
  <in_dir>/<series>_handbook_processing_loss.dta

Output (to --out_dir):
  <out_dir>/<series>_hb_processing_report.md
"""

import sys, shlex
import argparse
from pathlib import Path
import pandas as pd

STAGES = ["has_pdf", "has_eb_pages", "llm_csv", "has_eb_rows"]
LABELS = ["PDF present", "EB pages found", "CSV extracted", "Reliable EB rows"]

def ensure_binary(df, cols):
    for c in cols:
        df[c] = (df[c].fillna(0).astype(float) > 0).astype(int)

def funnel_keeps(df, stages):
    total = len(df)
    keeps = []
    kept = df
    for i, c in enumerate(stages):
        kept = kept[kept[c] == 1] if i else df[df[c] == 1]
        keeps.append(len(kept))
    return total, keeps

def fmt_pct(n, d): 
    return "0.00%" if d == 0 else f"{(100.0*n/d):.2f}%"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--in_dir",  default=".", help="Directory with <series>_handbook_processing_loss.dta")
    ap.add_argument("--out_dir", default=".", help="Directory to write <series>_hb_processing_report.md")

    # Handle Stata's single-blob args
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

    df = pd.read_stata(in_path)

    # Required columns
    s_id, s_nm = f"{series}_state_id", f"{series}_state_name"
    d_id, d_nm = f"{series}_district_id", f"{series}_district_name"
    fncol = "filename" if "filename" in df.columns else None
    for col in [s_id, s_nm, d_id, d_nm, *STAGES]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    ensure_binary(df, STAGES)

    # ---------- Overall funnel table ----------
    total, keeps = funnel_keeps(df, STAGES)
    drops_from_prev = [total - keeps[0]] + [keeps[i-1] - keeps[i] for i in range(1, len(keeps))]

    lines = []
    lines.append(f"# {series.upper()} Handbook Processing — Markdown Report\n")
    lines.append(f"_Input_: `{in_path}`  \n_Output_: `{out_path}`\n")

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

    # ---------- Drill-downs ----------
    def block(title, frame):
        lines.append(f"## {title}\n")
        if frame.empty:
            lines.append("_None_\n")
            return
        cols = [s_id, s_nm, d_id, d_nm] + ([fncol] if fncol else [])
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        for _, r in frame.sort_values([s_id, d_id]).iterrows():
            lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
        lines.append("")

    missing_pdf = df[df["has_pdf"].eq(0)]
    no_eb_pages = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(0)]
    no_csv      = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(0)]
    no_rows     = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(1) & df["has_eb_rows"].eq(0)]

    block("Missing PDFs", missing_pdf)
    block("No EB pages (given PDFs)", no_eb_pages)
    block("No CSV (given EB pages)", no_csv)
    block("No reliable EB rows (given CSV)", no_rows)

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
make_attrition_report.py

Generate a Markdown attrition report for handbook processing and downstream merges.

Stages included
---------------
1–4 (from <series>_handbook_processing_loss.dta)
  1) PDF present
  2) EB pages found
  3) CSV extracted
  4) Reliable EB rows

Excel-based EB sources (from raw Excel files, not LLM CSVs)
-----------------------------------------------------------
  • Counts and lists .xls/.xlsx workbooks in:
      ~/iec/pc01/district_handbooks/taha_2025_09_19

Town-level downstream (from your Stata pipeline)
------------------------------------------------
  A) Towns in the handbook panel (distinct handbook towns BEFORE fuzzy)
  B) Towns after fuzzy-match to urban PCA
     • Preferred source: <series>_towns_after_pca_matched.dta (immediately after `keep if match_source <= 4`)
     • Fallback:        <series>_combined_hb_w_pca_cln.dta (distinct `idm`)
  C) Towns with SHRID joined (distinct `idm`) and # of SHRIDs (distinct `shrid2` or `shrid`)
     • Source:          <series>_combined_hb_w_pca_shrid_cln.dta
  D) SHRIDs in PC01×PC11 panel (intersection of valid SHRIDs across both years)
     • Sources:         pc01_seg_sc_by_shrid.dta, pc11_seg_sc_by_shrid.dta

Inputs (by --series and --in_dir)
---------------------------------
Required:
  <in_dir>/<series>_handbook_processing_loss.dta
    Columns:
      <series>_state_id, <series>_state_name,
      <series>_district_id, <series>_district_name,
      has_pdf, has_eb_pages, llm_csv, has_eb_rows
    Optional: filename

Town-level files (produced by your Stata script):
  <in_dir>/<series>_town_hb_df.dta
  <in_dir>/<series>_towns_after_pca_matched.dta          (right after keep if match_source <= 4)
  <in_dir>/<series>_combined_hb_w_pca_cln.dta            (post EB merge-back; has idm)
  <in_dir>/<series>_combined_hb_w_pca_shrid_cln.dta      (after SHRID join; has idm & shrid2/shrid)

Panel files:
  <in_dir>/pc01_seg_sc_by_shrid.dta
  <in_dir>/pc11_seg_sc_by_shrid.dta

Output (to --out_dir; default ./data_loss/reports)
--------------------------------------------------
  <out_dir>/<series>_hb_processing_report.md
"""

import sys, shlex
import argparse
from pathlib import Path
import pandas as pd
from typing import Iterable, List, Optional, Tuple, Set, Union

# Stage 1..4 (must match upstream Stata)
STAGES = ["has_pdf", "has_eb_pages", "llm_csv", "has_eb_rows"]
LABELS = ["PDF present", "EB pages found", "CSV extracted", "Reliable EB rows"]


# ------------------------
# Small helper functions
# ------------------------
def ensure_binary(df: pd.DataFrame, cols: Iterable[str]) -> None:
    """Coerce stage columns to {0,1} (NaN→0; >0→1)."""
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce").fillna(0)
        df[c] = (s > 0).astype("int8")


def funnel_keeps(df: pd.DataFrame, stages: List[str]) -> Tuple[int, List[int]]:
    """Compute cumulative keeps at each stage."""
    total = len(df)
    keeps = []
    kept = df
    for i, c in enumerate(stages):
        kept = kept[kept[c] == 1] if i else df[df[c] == 1]
        keeps.append(len(kept))
    return total, keeps


def fmt_pct(n: int, d: int) -> str:
    return "0.00%" if d == 0 else f"{(100.0 * n / d):.2f}%"


def safe_read_stata(p: Path) -> Optional[pd.DataFrame]:
    """Read a Stata file if it exists; return None on failure."""
    try:
        if p.exists():
            return pd.read_stata(p)
    except Exception as e:
        print(f"[WARN] Could not read {p.name}: {e}")
    return None


def distinct_nonnull(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return df[col].dropna().astype(str).nunique()


def distinct_count(df: pd.DataFrame, cols: List[str]) -> int:
    """Distinct count on a list of columns (as strings); skips if any missing."""
    for c in cols:
        if c not in df.columns:
            return 0
    sub = df[cols].copy()
    for c in cols:
        sub[c] = sub[c].astype(str)
    return len(sub.drop_duplicates())


def first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def valid_panel_shrids(df_year: pd.DataFrame, suffix: str) -> Tuple[Set[str], Optional[str]]:
    """
    From a *_seg_sc_by_shrid.dta file, return the set of SHRIDs that have both
    d_sc_* and iso_sc_* non-missing. Also returns which SHRID column name was used.
    """
    dcol = f"d_sc_{suffix}"
    icol = f"iso_sc_{suffix}"
    if dcol not in df_year.columns or icol not in df_year.columns:
        return set(), None
    shrid_col = "shrid2" if "shrid2" in df_year.columns else ("shrid" if "shrid" in df_year.columns else None)
    if shrid_col is None or shrid_col not in df_year.columns:
        return set(), None
    v = df_year[dcol].notna() & df_year[icol].notna()
    return set(df_year.loc[v, shrid_col].astype(str)), shrid_col


# --------------
# Main program
# --------------
def main():
    # -------- args --------
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--in_dir",  default=".", help="Dir with <series>_*.dta")
    ap.add_argument("--out_dir", default="./data_loss/reports", help="Dir to write <series>_hb_processing_report.md")
    ap.add_argument("--title",   default=None, help="Optional report title override")

    # Handle single-blob argv (e.g., from Stata/Make)
    argv = sys.argv[1:]
    if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
        argv = shlex.split(argv[0])
    args = ap.parse_args(argv)

    series = args.series
    in_dir  = Path(args.in_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # -------- paths --------
    loss_path          = in_dir / f"{series}_handbook_processing_loss.dta"
    town_universe_path = in_dir / f"{series}_town_hb_df.dta"
    towns_after_path   = in_dir / f"{series}_towns_after_pca_matched.dta"
    eb_pca_path        = in_dir / f"{series}_combined_hb_w_pca_cln.dta"
    shrid_cln_path     = in_dir / f"{series}_combined_hb_w_pca_shrid_cln.dta"
    pc01_seg_path      = in_dir / "pc01_seg_sc_by_shrid.dta"
    pc11_seg_path      = in_dir / "pc11_seg_sc_by_shrid.dta"

    # Raw Excel workbooks (Excel EB sources, independent of --in_dir)
    excel_dir = Path("~/iec/pc01/district_handbooks/taha_2025_09_19").expanduser()

    out_path = out_dir / f"{series}_hb_processing_report.md"
    if not loss_path.exists():
        raise FileNotFoundError(f"Required input not found: {loss_path}")

    # -------- load Stage 1..4 table --------
    df = pd.read_stata(loss_path)

    s_id, s_nm = f"{series}_state_id",    f"{series}_state_name"
    d_id, d_nm = f"{series}_district_id", f"{series}_district_name"
    fncol = "filename" if "filename" in df.columns else None

    for col in [s_id, s_nm, d_id, d_nm, *STAGES]:
        if col not in df.columns:
            raise ValueError(f"Missing column in {loss_path.name}: {col}")

    ensure_binary(df, STAGES)

    # -------- overall funnel --------
    total, keeps = funnel_keeps(df, STAGES)

    # -------- count & list raw Excel workbooks --------
    excel_files: List[Path] = []
    excel_note = ""
    if excel_dir.exists() and excel_dir.is_dir():
        for pattern in ("*.xls", "*.xlsx"):
            excel_files.extend(excel_dir.glob(pattern))
        excel_files = sorted(p for p in excel_files if p.is_file())
        excel_note = f"`.xls`/`.xlsx` files in `{excel_dir}`"
    else:
        excel_note = f"`{excel_dir}` not found or not a directory"

    excel_count = len(excel_files) if excel_files else None

    # -------- build markdown --------
    lines: List[str] = []
    title = args.title or f"{series.upper()} Handbook Processing — Markdown Report"
    lines.append(f"# {title}\n")

    # Main funnel
    lines.append("## Overall Attrition Funnel (PDF → EB pages → CSV → Reliable rows)\n")
    lines.append("| Stage | Kept | % of Total | Dropped from Prev |")
    lines.append("|---|---:|---:|---:|")
    prev_kept = total
    for i, label in enumerate(LABELS):
        kept = keeps[i]
        dropped = prev_kept - kept
        lines.append(f"| {i+1}. {label} | {kept} | {fmt_pct(kept, total)} | {dropped} |")
        prev_kept = kept
    lines.append("")

    # New section: Excel-based EB sources (summary + list)
    lines.append("## Excel-based EB Sources\n")
    lines.append("| Metric | Count | Note |")
    lines.append("|---|---:|---|")
    if excel_count is not None:
        lines.append(f"| Raw Excel handbooks (EB tables) | {excel_count} | {excel_note} |")
    else:
        lines.append(f"| Raw Excel handbooks (EB tables) | _n/a_ | {excel_note} |")
    lines.append("")

    if excel_count:
        lines.append("### Raw Excel Workbooks Used for EB Extraction\n")
        lines.append("| # | Workbook filename |")
        lines.append("|---:|---|")
        for i, p in enumerate(excel_files, start=1):
            lines.append(f"| {i} | `{p.name}` |")
        lines.append("")

    # ================================
    # Town-level downstream attrition
    # (placed BEFORE drill-down lists)
    # ================================
    lines.append("## Town-level Downstream Coverage\n")

    # A) Universe of handbook towns (pre-fuzzy)
    df_town_universe = safe_read_stata(town_universe_path)
    if df_town_universe is None:
        # minimal fallback: try pre-merge file that still has town_hb
        fallback_path = in_dir / f"{series}_combined_hb_w_key.dta"
        df_town_universe = safe_read_stata(fallback_path)
        town_universe_note = f"`{town_universe_path.name}` not found; fallback `{fallback_path.name}`"
    else:
        town_universe_note = f"`{town_universe_path.name}`"

    if df_town_universe is not None:
        # Use distinct across (<series>_state_id, <series>_district_id, <series>_town_hb)
        pre_cols = [s_id, d_id, f"{series}_town_hb"]
        # Gracefully degrade if any column is missing
        pre_cols = [c for c in pre_cols if c in df_town_universe.columns]
        towns_universe = 0
        if len(pre_cols) >= 2:  # need at least ID + name to be meaningful
            towns_universe = distinct_count(df_town_universe, pre_cols)
        lines.append("| Metric | Count | Note |")
        lines.append("|---|---:|---|")
        lines.append(f"| A. Towns in handbook panel (pre-fuzzy) | {towns_universe} | distinct {', '.join('`'+c+'`' for c in pre_cols)} in {town_universe_note} |")
    else:
        lines.append("| Metric | Count | Note |")
        lines.append("|---|---:|---|")
        lines.append(f"| A. Towns in handbook panel (pre-fuzzy) | _n/a_ | no suitable source found |")

    # B) Towns after fuzzy-match to urban PCA
    df_towns_after = safe_read_stata(towns_after_path)
    if df_towns_after is not None:
        # Prefer idm, otherwise use (<series>_state_id, <series>_district_id, <series>_town_id) or std_town
        id_cols_priority = [
            ["idm"],
            [s_id, d_id, f"{series}_town_id"],
            [s_id, d_id, "std_town"],
        ]
        towns_after_fuzzy = 0
        used_cols = None
        for cols in id_cols_priority:
            if all(c in df_towns_after.columns for c in cols):
                towns_after_fuzzy = distinct_count(df_towns_after, cols)
                used_cols = cols
                break
        if used_cols:
            note_b = f"`{towns_after_path.name}` using " + ", ".join(f"`{c}`" for c in used_cols)
        else:
            note_b = f"`{towns_after_path.name}` (no suitable ID columns found)"
    else:
        # fallback to post-merge-back file (will generally be close, but not identical)
        df_eb_pca = safe_read_stata(eb_pca_path)
        if df_eb_pca is not None:
            if "idm" in df_eb_pca.columns:
                towns_after_fuzzy = distinct_nonnull(df_eb_pca, "idm")
                note_b = f"`{eb_pca_path.name}` `idm` (fallback)"
            else:
                comp_cols = [s_id, d_id, f"{series}_town_id"]
                towns_after_fuzzy = distinct_count(df_eb_pca, comp_cols)
                note_b = f"`{eb_pca_path.name}` composite (fallback)"
        else:
            towns_after_fuzzy = 0
            note_b = f"`{towns_after_path.name}` not found; no fallback available"

    lines.append(f"| B. Towns after fuzzy-match to urban PCA | {towns_after_fuzzy} | {note_b} |")

    # C) Towns with SHRID joined  — and number of SHRIDs
    df_shrid_cln = safe_read_stata(shrid_cln_path)
    if df_shrid_cln is not None:
        towns_with_shrid = distinct_nonnull(df_shrid_cln, "idm")
        shrid_col = first_existing(df_shrid_cln, ["shrid2", "shrid"])
        num_shrids = distinct_nonnull(df_shrid_cln, shrid_col) if shrid_col else 0
        lines.append(f"| C1. Towns with SHRID joined | {towns_with_shrid} | `{shrid_cln_path.name}` distinct `idm` |")
        lines.append(f"| C2. # of SHRIDs after join | {num_shrids} | `{shrid_cln_path.name}` distinct `{shrid_col or 'shrid2/shrid'}` |")
    else:
        lines.append(f"| C1. Towns with SHRID joined | _n/a_ | `{shrid_cln_path.name}` not found |")
        lines.append(f"| C2. # of SHRIDs after join | _n/a_ | `{shrid_cln_path.name}` not found |")

    # D) SHRIDs in PC01×PC11 panel (intersection of valid SHRIDs across both years)
    df_pc01 = safe_read_stata(pc01_seg_path)
    df_pc11 = safe_read_stata(pc11_seg_path)
    if df_pc01 is not None and df_pc11 is not None:
        s01, col01 = valid_panel_shrids(df_pc01, "pc01")
        s11, col11 = valid_panel_shrids(df_pc11, "pc11")
        panel_shrids = s01 & s11
        lines.append(f"| D. SHRIDs in PC01×PC11 panel | {len(panel_shrids)} | intersection of valid SHRIDs from `{pc01_seg_path.name}` and `{pc11_seg_path.name}` |")
    else:
        missing = []
        if df_pc01 is None: missing.append(pc01_seg_path.name)
        if df_pc11 is None: missing.append(pc11_seg_path.name)
        lines.append(f"| D. SHRIDs in PC01×PC11 panel | _n/a_ | missing: {', '.join('`'+m+'`' for m in missing)} |")

    lines.append("")

    # -------- stage drill-downs (AFTER town-level section) --------
    def block(title: str, frame: pd.DataFrame):
        lines.append(f"## {title}\n")
        if frame.empty:
            lines.append("_None_\n")
            return
        cols = [s_id, s_nm, d_id, d_nm] + ([fncol] if fncol else [])
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        for _, r in frame.sort_values([s_id, d_id]).iterrows():
            vals = [str(r.get(c, "")) if pd.notna(r.get(c, "")) else "" for c in cols]
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

    missing_pdf = df[df["has_pdf"].eq(0)]
    no_eb_pages = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(0)]
    no_csv      = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(0)]
    no_rows     = df[df["has_pdf"].eq(1) & df["has_eb_pages"].eq(1) & df["llm_csv"].eq(1) & df["has_eb_rows"].eq(0)]

    block("Missing PDFs", missing_pdf)
    block("No EB pages (given PDFs)", no_eb_pages)
    block("No CSV (given EB pages)", no_csv)
    block("No reliable EB rows (given CSV)", no_rows)

    # -------- write --------
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path} (generated by make_attrition_report.py)")


if __name__ == "__main__":
    main()

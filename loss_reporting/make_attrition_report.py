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

Town-level downstream (optional, auto-detected)
  5) Towns fuzzy-matched to urban PCA
  6) Towns with SHRID joined
  7) Towns that are in the PC01×PC11 panel (via SHRID intersection)

Required input (by --series and --in_dir)
-----------------------------------------
<in_dir>/<series>_handbook_processing_loss.dta
  Must include:
    <series>_state_id, <series>_state_name,
    <series>_district_id, <series>_district_name,
    has_pdf, has_eb_pages, llm_csv, has_eb_rows
  Optional: filename

Optional downstream inputs (auto-detected if present)
-----------------------------------------------------
<in_dir>/<series>_combined_hb_w_pca_cln.dta
<in_dir>/<series>u_shrid_key.dta
<in_dir>/pc01_seg_sc_by_shrid.dta
<in_dir>/pc11_seg_sc_by_shrid.dta

Output (to --out_dir)
---------------------
<out_dir>/<series>_hb_processing_report.md
(default out_dir: ./data_loss/reports)
"""

import sys, shlex
import argparse
from pathlib import Path
import pandas as pd

# Stage 1..4 (must match upstream Stata)
STAGES = ["has_pdf", "has_eb_pages", "llm_csv", "has_eb_rows"]
LABELS = ["PDF present", "EB pages found", "CSV extracted", "Reliable EB rows"]

def ensure_binary(df: pd.DataFrame, cols) -> None:
    """Coerce stage columns to {0,1} (NaN→0; >0→1)."""
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce").fillna(0)
        df[c] = (s > 0).astype("int8")

def funnel_keeps(df: pd.DataFrame, stages):
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

def safe_read_stata(p: Path) -> pd.DataFrame | None:
    """Read a Stata file if it exists; return None on failure."""
    try:
        if p.exists():
            return pd.read_stata(p)
    except Exception as e:
        print(f"[WARN] Could not read {p.name}: {e}")
    return None

def main():
    # -------- args --------
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--in_dir",  default=".", help="Dir with <series>_*.dta")
    ap.add_argument("--out_dir", default="./data_loss/reports",
                    help="Dir to write <series>_hb_processing_report.md (default: ./data_loss/reports)")
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

    in_path  = in_dir / f"{series}_handbook_processing_loss.dta"
    out_path = out_dir / f"{series}_hb_processing_report.md"
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    # -------- load Stage 1..4 table --------
    df = pd.read_stata(in_path)

    s_id, s_nm = f"{series}_state_id",    f"{series}_state_name"
    d_id, d_nm = f"{series}_district_id", f"{series}_district_name"
    fncol = "filename" if "filename" in df.columns else None

    for col in [s_id, s_nm, d_id, d_nm, *STAGES]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    ensure_binary(df, STAGES)

    # -------- overall funnel --------
    total, keeps = funnel_keeps(df, STAGES)

    lines = []
    title = args.title or f"{series.upper()} Handbook Processing — Markdown Report"
    lines.append(f"# {title}\n")

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

    # ================================
    # Town-level downstream coverage (moved up)
    # ================================
    eb_pca_path    = in_dir / f"{series}_combined_hb_w_pca_cln.dta"
    shrid_key_path = in_dir / f"{series}u_shrid_key.dta"
    pc01_seg_path  = in_dir / "pc01_seg_sc_by_shrid.dta"
    pc11_seg_path  = in_dir / "pc11_seg_sc_by_shrid.dta"

    df_eb_pca = safe_read_stata(eb_pca_path)
    df_key    = safe_read_stata(shrid_key_path)
    df_pc01   = safe_read_stata(pc01_seg_path)
    df_pc11   = safe_read_stata(pc11_seg_path)

    if df_eb_pca is not None:
        town_id_col = f"{series}_town_id" if f"{series}_town_id" in df_eb_pca.columns else None
        if town_id_col is None:
            print(f"[INFO] {eb_pca_path.name} has no '{series}_town_id' column; skipping town-level section.")
        else:
            # 5) Towns fuzzy-matched to PCA
            towns_fuzzy = set(df_eb_pca[town_id_col].dropna().astype(str))

            # 6) Towns with SHRID joined
            towns_with_shrid = None
            if df_key is not None:
                key_town_col = (
                    town_id_col if town_id_col in df_key.columns else
                    (f"{series}_town_id" if f"{series}_town_id" in df_key.columns else
                     ("town_id" if "town_id" in df_key.columns else None))
                )
                if key_town_col:
                    key_towns = set(df_key[key_town_col].dropna().astype(str))
                    towns_with_shrid = towns_fuzzy & key_towns

            # 7) Towns in PC01×PC11 panel (via SHRID intersection)
            towns_in_panel = None
            if (df_key is not None) and (df_pc01 is not None) and (df_pc11 is not None):
                def valid_shrids(df_year, suffix):
                    dcol = f"d_sc_{suffix}"
                    icol = f"iso_sc_{suffix}"
                    key  = "shrid2" if "shrid2" in df_year.columns else "shrid"
                    v = df_year[dcol].notna() & df_year[icol].notna()
                    return set(df_year.loc[v, key].astype(str)), key

                s01, _ = valid_shrids(df_pc01, "pc01")
                s11, _ = valid_shrids(df_pc11, "pc11")
                panel_shrids = s01 & s11

                shrid_col = "shrid2" if "shrid2" in df_key.columns else ("shrid" if "shrid" in df_key.columns else None)
                if shrid_col:
                    key_town_col = (
                        town_id_col if town_id_col in df_key.columns else
                        (f"{series}_town_id" if f"{series}_town_id" in df_key.columns else "town_id")
                    )
                    sub = df_key[[key_town_col, shrid_col]].dropna()
                    sub[key_town_col] = sub[key_town_col].astype(str)
                    sub[shrid_col]    = sub[shrid_col].astype(str)
                    sub = sub[sub[key_town_col].isin(towns_fuzzy)]
                    towns_in_panel = set(sub.loc[sub[shrid_col].isin(panel_shrids), key_town_col].astype(str))

            # Emit section
            lines.append("## Town-level Downstream Coverage\n")
            lines.append("| Metric | Count | Note |")
            lines.append("|---|---:|---|")
            lines.append(f"| Towns fuzzy-matched to urban PCA | {len(towns_fuzzy)} | distinct `{town_id_col}` in `{eb_pca_path.name}` |")
            if towns_with_shrid is not None:
                lines.append(f"| Towns with SHRID joined | {len(towns_with_shrid)} | intersection with `{shrid_key_path.name}` |")
            else:
                lines.append(f"| Towns with SHRID joined | _n/a_ | `{shrid_key_path.name}` not found |")
            if towns_in_panel is not None:
                lines.append(f"| Towns in PC01×PC11 panel | {len(towns_in_panel)} | SHRID present in both `pc01` and `pc11` valid sets |")
            else:
                lines.append(f"| Towns in PC01×PC11 panel | _n/a_ | need `pc01_seg_sc_by_shrid.dta` and `pc11_seg_sc_by_shrid.dta` |")
            lines.append("")

    # -------- drill-downs (now after town section) --------
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

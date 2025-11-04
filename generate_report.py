#!/usr/bin/env python3
"""
make_uncovered_markdown.py

Generates a Markdown report listing states and districts from the PCA (by series)
that have at least one town >= 1,000 population but are NOT covered by the
district handbooks.

Inputs (Stata .dta):
  - PCA file: e.g., pc01u_pca_clean.dta or pc11u_pca_clean.dta
  - Handbook uniq state-district file: e.g., ${series}_hb_uniq_state_dist_w_key.dta

Usage:
  python make_uncovered_markdown.py \
    --series pc01 \
    --pca /path/to/pc01u_pca_clean.dta \
    --hb  /path/to/pc01_hb_uniq_state_dist_w_key.dta \
    --out /path/to/uncovered_pc01.md
"""

import argparse
import unicodedata
import pandas as pd
from pathlib import Path

def name_clean(s):
    """Mimic a light 'name_clean': lowercase, strip, collapse spaces, remove exotic whitespace."""
    if pd.isna(s):
        return s
    # Normalize unicode and remove control characters
    s = unicodedata.normalize("NFKC", str(s))
    # Trim + collapse spaces
    s = " ".join(s.strip().split())
    # Lowercase
    s = s.lower()
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01", "pc11", "pc91"],
                    help="Census series (pc01, pc11, pc91)")
    ap.add_argument("--pca", required=True, help="Path to {series}u_pca_clean.dta")
    ap.add_argument("--hb",  required=True, help="Path to {series}_hb_uniq_state_dist_w_key.dta")
    ap.add_argument("--out", required=True, help="Output Markdown path")
    args = ap.parse_args()

    series = args.series
    pca_path = Path(args.pca).expanduser().resolve()
    hb_path  = Path(args.hb).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    # Column stems that depend on series
    state_id_col      = f"{series}_state_id"
    dist_id_col       = f"{series}_district_id"
    state_name_col    = f"{series}_state_name"
    dist_name_col     = f"{series}_district_name"
    pca_pop_col       = f"{series}_pca_tot_p"

    # Load PCA towns
    pca = pd.read_stata(pca_path)

    # Coerce IDs to numeric (destring)
    for c in [state_id_col, dist_id_col]:
        if c in pca.columns:
            pca[c] = pd.to_numeric(pca[c], errors="coerce")

    # Town-level flag: population >= 1000 (ignore missing)
    if pca_pop_col not in pca.columns:
        raise ValueError(f"Expected column '{pca_pop_col}' not found in PCA file.")
    pca["town1000"] = (pd.to_numeric(pca[pca_pop_col], errors="coerce") >= 1000)

    # District-level: keep districts with >=1 such town
    grp_keys = [state_id_col, dist_id_col, state_name_col, dist_name_col]
    # Some files may lack names (rare). Keep what exists.
    grp_keys = [k for k in grp_keys if k in pca.columns]
    has_big = (
        pca.groupby([state_id_col, dist_id_col], dropna=False)["town1000"]
           .max()
           .reset_index()
           .rename(columns={"town1000": "has_big_town"})
    )
    # One row per district with ID keys + names
    # Merge back a single representative row for names (first)
    first_names = (
        pca.sort_values(grp_keys)
           .drop_duplicates(subset=[state_id_col, dist_id_col])
           [ [c for c in grp_keys] ]
    )
    pca_districts = has_big.merge(first_names, on=[state_id_col, dist_id_col], how="left")
    pca_districts = pca_districts[pca_districts["has_big_town"] == True].copy()

    # Load HB uniq state-district mapping (coverage universe)
    hb = pd.read_stata(hb_path)
    # Coerce IDs
    for c in [state_id_col, dist_id_col]:
        if c in hb.columns:
            hb[c] = pd.to_numeric(hb[c], errors="coerce")

    hb_keys = [state_id_col, dist_id_col]
    # Ensure uniqueness at state-district level in HB file
    hb_uniq = hb.drop_duplicates(subset=hb_keys).copy()

    # Compute matches / coverage
    merged = pca_districts.merge(hb_uniq[hb_keys], on=hb_keys, how="left", indicator=True)
    matched_mask = merged["_merge"] == "both"
    matched = merged[matched_mask]
    total = len(merged)
    n_matched = matched.shape[0]
    pct = (n_matched / total * 100) if total > 0 else 0.0

    # Uncovered districts: in PCA big-town universe but not in HB
    uncovered = merged[~matched_mask].copy()

    # For Markdown, we’ll present by state (name if available, else ID)
    def state_label(row):
        if state_name_col in uncovered.columns and pd.notna(row.get(state_name_col)):
            return str(row[state_name_col])
        return f"state_{int(row[state_id_col])}" if pd.notna(row[state_id_col]) else "state_unknown"

    def dist_label(row):
        if dist_name_col in uncovered.columns and pd.notna(row.get(dist_name_col)):
            return str(row[dist_name_col])
        return f"district_{int(row[dist_id_col])}" if pd.notna(row[dist_id_col]) else "district_unknown"

    uncovered["__state_label"] = uncovered.apply(state_label, axis=1)
    uncovered["__dist_label"]  = uncovered.apply(dist_label, axis=1)

    # Group for output
    groups = (
        uncovered
        .sort_values(["__state_label", "__dist_label"])
        .groupby("__state_label", dropna=False)
    )

    # Build Markdown
    lines = []
    lines.append(f"# Districts NOT Covered by Handbooks — {series.upper()}")
    lines.append("")
    lines.append(f"**Coverage:** {n_matched} / {total} districts "
                 f"({pct:.1f}%) of PCA districts with ≥1 town ≥1,000 people are covered by handbooks.")
    lines.append("")
    if uncovered.empty:
        lines.append("_All eligible PCA districts appear in the handbook coverage set._")
    else:
        state_count = 0
        dist_count = 0
        for state, g in groups:
            state_count += 1
            lines.append(f"## {state}")
            for _, r in g.iterrows():
                dist_count += 1
                sid = r[state_id_col]
                did = r[dist_id_col]
                lines.append(f"- {r['__dist_label']}  "
                             f"(state_id={int(sid) if pd.notna(sid) else 'NA'}, "
                             f"district_id={int(did) if pd.notna(did) else 'NA'})")
            lines.append("")  # blank line between states
        lines.insert(3, f"_Uncovered states: {state_count} • Uncovered districts: {dist_count}_")
        lines.insert(4, "")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"Wrote Markdown to: {out_path}")
    print(f"Matched {n_matched} of {total} districts ({pct:.1f}%).")
    if not uncovered.empty:
        print(f"Uncovered states: {uncovered['__state_label'].nunique()}, "
              f"Uncovered districts: {uncovered.shape[0]}")

if __name__ == "__main__":
    main()

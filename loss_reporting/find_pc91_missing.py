#!/usr/bin/env python3
"""
find_pc91_missing.py

Create a single Markdown report that shows:
  (1) The pc91 district handbooks we DO have (folder + keys), and
  (2) The pc91 urban PCA districts we DO NOT have (missing a handbook).

Assumptions
- Handbook keys CSV has: filename, state_name, state_id, district_name, district_id
- PCA .dta has: pc91_state_name, pc91_state_id, pc91_district_name, pc91_district_id

The script writes ONLY the Markdown file and produces no other output.
"""

from pathlib import Path
import os
import pandas as pd

# -----------------------------
# Paths (adjust if needed)
# -----------------------------
PDF_DIR = Path("/dartfs/rc/lab/I/IEC/pc91/district_handbooks")  # non-recursive
KEYS_CSV = Path("~/ddl/india-census-district-handbooks/data/pc91_hb_pdf_keys.csv").expanduser()
PCA_DTA  = Path("/dartfs-hpc/rc/home/w/f0083xw/iec/pc91/pc91u_pca_clean.dta")
MARKDOWN_REPORT_OUT = Path("/dartfs-hpc/rc/home/w/f0083xw/ddl/india-census-district-handbooks/loss_reporting/reports/pc91_handbook_coverage_report.md")

# -----------------------------
# Helpers
# -----------------------------
def list_pdfs_nonrecursive(pdf_dir: Path) -> pd.DataFrame:
    pdfs = sorted([n for n in os.listdir(pdf_dir) if n.lower().endswith(".pdf")]) if pdf_dir.is_dir() else []
    return pd.DataFrame({"filename": pdfs})

def coerce_to_Int64(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def merge_filenames_with_keys(files_df: pd.DataFrame, keys_csv: Path) -> pd.DataFrame:
    keys_df = pd.read_csv(keys_csv)

    # Normalize filename column name to 'filename'
    filename_col_candidates = ["filename", "file_name", "pdf_name", "pdf_filename"]
    filename_col = next((c for c in filename_col_candidates if c in keys_df.columns), None)
    if filename_col is None:
        raise KeyError(f"Keys file must include one of: {filename_col_candidates}")
    if filename_col != "filename":
        keys_df = keys_df.rename(columns={filename_col: "filename"})

    # Ensure required id/name columns exist on keys (handbooks side)
    required_hb_cols = ["state_name", "state_id", "district_name", "district_id"]
    missing = [c for c in required_hb_cols if c not in keys_df.columns]
    if missing:
        raise KeyError(f"Keys file missing required columns: {missing}")

    merged = files_df.merge(keys_df, on="filename", how="left", indicator=True)
    return merged

def load_pca_unique(pca_dta: Path) -> pd.DataFrame:
    pca = pd.read_stata(pca_dta)
    expected = ["pc91_state_name", "pc91_state_id", "pc91_district_name", "pc91_district_id"]
    missing = [c for c in expected if c not in pca.columns]
    if missing:
        raise KeyError(f"PCA file missing expected columns: {missing}")

    pca = pca.rename(columns={
        "pc91_state_name": "state_name",
        "pc91_state_id": "state_id",
        "pc91_district_name": "district_name",
        "pc91_district_id": "district_id",
    })
    pca_u = (
        pca[["state_name", "state_id", "district_name", "district_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    pca_u = coerce_to_Int64(pca_u, ["state_id", "district_id"])
    pca_u = pca_u.sort_values(["state_id", "district_id"])
    return pca_u

def build_have_list(hb_keys: pd.DataFrame) -> pd.DataFrame:
    have = hb_keys.loc[
        hb_keys["_merge"] == "both",
        ["filename", "state_name", "district_name", "state_id", "district_id"]
    ].copy()
    have = coerce_to_Int64(have, ["state_id", "district_id"])
    have = have.sort_values(["state_id", "district_id", "filename"]).reset_index(drop=True)
    return have

def build_missing_from_pca(have: pd.DataFrame, pca_u: pd.DataFrame) -> pd.DataFrame:
    have_keys = have[["state_id", "district_id"]].drop_duplicates()
    have_keys = coerce_to_Int64(have_keys, ["state_id", "district_id"])
    pca_u = coerce_to_Int64(pca_u.copy(), ["state_id", "district_id"])

    merged = pca_u.merge(have_keys, on=["state_id", "district_id"], how="left", indicator=True)
    missing = merged.loc[
        merged["_merge"] == "left_only",
        ["state_name", "state_id", "district_name", "district_id"]
    ].copy()
    missing = missing.sort_values(["state_id", "district_id"]).reset_index(drop=True)
    return missing

def dataframe_to_markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    sub = df[columns].fillna("").astype(str)
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = ["| " + " | ".join(map(str, row)) + " |" for row in sub.to_numpy()]
    return "\n".join([header, divider] + rows)

def write_markdown_report(have: pd.DataFrame, missing: pd.DataFrame, outfile) -> None:
    out_path = Path(outfile)  # accept str or Path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# Missing pc91 district handbooks")
    lines.append("")
    lines.append("## Handbooks we DO have (from folder + keys)")
    lines.append("")
    if len(have) == 0:
        lines.append("_None found._")
    else:
        lines.append(dataframe_to_markdown_table(have, ["filename", "state_name", "district_name"]))
    lines.append("")
    lines.append("## PCA districts we DO NOT have a handbook for")
    lines.append("")
    if len(missing) == 0:
        lines.append("_None missing — all PCA districts have a handbook match._")
    else:
        lines.append(dataframe_to_markdown_table(missing, ["state_name", "state_id", "district_name", "district_id"]))
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Main
# -----------------------------
def main():
    files_df = list_pdfs_nonrecursive(PDF_DIR)
    files_keys = merge_filenames_with_keys(files_df, KEYS_CSV)
    pca_u = load_pca_unique(PCA_DTA)
    have = build_have_list(files_keys)
    missing = build_missing_from_pca(have, pca_u)
    write_markdown_report(have, missing, MARKDOWN_REPORT_OUT)

if __name__ == "__main__":
    main()

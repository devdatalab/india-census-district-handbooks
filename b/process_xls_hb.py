#!/usr/bin/env python3
"""
INPUTS:
  - ~/iec/pcXX/district_handbooks/*.xls
  - ~/iec/pcXX/district_handbooks/*.xlsx
  - ~/iec/pcXX/district_handbooks_xii_b/*.xls
  - ~/iec/pcXX/district_handbooks_xii_b/*.xlsx
OUTPUTS:
  - ~/iec/pcXX/district_handbooks/eb_table_extracts/*.csv
  - ~/iec/pcXX/district_handbooks_xii_b/eb_table_extracts/*.csv
"""
# process_xls_hb.py  — detect EB rows by pattern, not headers

from pathlib import Path
import argparse, sys, shlex, re
import pandas as pd

TARGET_COLUMNS = ["location_code","town_hb","ward_name","eb_no","total_pop","sc_pop","st_pop"]
STRING_COLS = ["location_code","town_hb","ward_name","eb_no"]
INT_COLS    = ["total_pop","sc_pop","st_pop"]

# ---------- normalization helpers ----------
NBSP = "\u00A0"

def norm_cell(x) -> str:
    s = "" if x is None else str(x)
    s = s.replace(NBSP, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_1234567_row(vals) -> bool:
    toks = [re.sub(r"\D", "", v) for v in (norm_cell(v) for v in vals)]
    toks = [t for t in toks if t]
    return "".join(toks) == "1234567" or set(toks) == {"1","2","3","4","5","6","7"}

# ---------- validators for a 7-cell window ----------
LOC_RE   = re.compile(r"^\d{6,}$")
WARD_RE  = re.compile(r"^\s*ward\b", re.IGNORECASE)
EB_RE    = re.compile(r"\bEB\b", re.IGNORECASE)
NUM_RE   = re.compile(r"^\s*\d{1,3}(?:,\d{3})*\s*$")  # 1,087 etc.

def looks_like_int_or_dash(s: str) -> bool:
    s = s.strip()
    return s == "-" or bool(NUM_RE.match(s)) or s == ""

def window_matches(w):  # w is list[str] of length 7
    lc, town, ward, eb, tot, sc, st = w
    return (
        bool(LOC_RE.match(lc))
        and bool(WARD_RE.match(ward))
        and bool(EB_RE.search(eb))
        and looks_like_int_or_dash(tot)
        and looks_like_int_or_dash(sc)
        and looks_like_int_or_dash(st)
    )

def extract_window(vals):
    """
    Given a row (list of cells), slide a 7-cell window and return the first
    window matching our EB pattern, normalized to strings.
    """
    cells = [norm_cell(v) for v in vals]
    n = len(cells)
    for i in range(0, max(0, n - 6)):
        w = cells[i:i+7]
        if window_matches(w):
            return w
    return None

def to_int64(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.replace(",", "", regex=False).str.strip()
    s = s.replace({"-": pd.NA, "": pd.NA})
    return pd.to_numeric(s, errors="coerce").astype("Int64")

# ---------- IO ----------
def read_all_sheets(path: Path) -> list[pd.DataFrame]:
    dfs = []
    try:
        xf = pd.ExcelFile(path)  # let pandas choose engine
    except Exception:
        xf = None
        for eng in (None, "xlrd", "openpyxl"):
            try:
                xf = pd.ExcelFile(path, engine=eng)
                break
            except Exception:
                pass
        if xf is None:
            raise
    for name in xf.sheet_names:
        for eng in (None, "xlrd", "openpyxl"):
            try:
                df = pd.read_excel(path, sheet_name=name, header=None, dtype=str, engine=eng)
                dfs.append(df)
                break
            except Exception:
                continue
    return dfs

# ---------- core cleaning ----------
def process_workbook(xls_path: Path) -> pd.DataFrame:
    out_rows = []
    for df in read_all_sheets(xls_path):
        if df is None or df.empty:
            continue
        # normalize NBSPs early
        df = df.applymap(norm_cell)

        for _, row in df.iterrows():
            vals = row.tolist()
            if is_1234567_row(vals):
                continue
            w = extract_window(vals)
            if w is None:
                continue
            lc, town, ward, eb, tot, sc, st = w
            out_rows.append(
                {
                    "location_code": lc,
                    "town_hb": town,
                    "ward_name": ward,
                    "eb_no": eb,
                    "total_pop": tot,
                    "sc_pop": sc,
                    "st_pop": st,
                }
            )

    if not out_rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    out = pd.DataFrame(out_rows, columns=TARGET_COLUMNS)

    # keep only rows that contain "EB" in eb_no (extra guard)
    out = out[out["eb_no"].astype(str).str.contains(r"\bEB\b", case=False, na=False)]

    # dtypes
    for c in STRING_COLS:
        out[c] = out[c].astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
        out[c] = out[c].where(out[c] != "", pd.NA)
    for c in INT_COLS:
        out[c] = to_int64(out[c])

    # basic sanity on location code
    ok_lc = out["location_code"].astype("string").str.match(r"^\d{6,}$", na=False)
    out = out[ok_lc].copy()

    # final order/dtypes
    out = out[TARGET_COLUMNS]
    for c in STRING_COLS: out[c] = out[c].astype("string")
    for c in INT_COLS:    out[c] = out[c].astype("Int64")
    out = out.dropna(how="all")

    return out

def main():
    ap = argparse.ArgumentParser(description="Process EB XLS/XLSX into cleaned CSVs by pattern.")
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--pdf_root", required=True, help="Root with eb_table_extracts/.")
    ap.add_argument("--xls_source_directory", default="",
                    help="Optional subfolder under --pdf_root containing XLS/XLSX. If empty, reads from --pdf_root.")
    argv = sys.argv[1:]
    if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]): argv = shlex.split(argv[0])
    args = ap.parse_args(argv)

    pdf_root = Path(args.pdf_root)
    src_dir  = pdf_root / args.xls_source_directory if args.xls_source_directory.strip() else pdf_root
    out_dir  = pdf_root / "eb_table_extracts"
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(list(src_dir.glob("*.xls")) + list(src_dir.glob("*.xlsx")))
    if not files:
        print(f"[INFO] No .xls/.xlsx found in {src_dir}")
        return

    ok = fail = 0
    for f in files:
        try:
            out = process_workbook(f)
            out.to_csv(out_dir / (f.stem + ".csv"), index=False)
            print(f"[OK]  {f.name} → {f.stem}.csv (rows: {len(out)})")
            ok += 1
        except Exception as e:
            print(f"[ERR] {f.name}: {type(e).__name__}: {e}")
            fail += 1

    print(f"\n[SUMMARY] cleaned={ok}, failed={fail}, out_dir={out_dir}")

if __name__ == "__main__":
    main()

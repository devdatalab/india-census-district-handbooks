#!/usr/bin/env python3
# hb_loss_report.py — builds a Markdown attrition report
# Input:  <in_dir>/<series>_handbook_processing_loss.dta
# Output: <out_dir>/<series>_hb_processing_report.md

import argparse
from pathlib import Path
import pandas as pd

# ---- helpers (shortened) ----
def detect_columns(df, series):
    need = {
        "state_id": f"{series}_state_id",
        "state_name": f"{series}_state_name",
        "district_id": f"{series}_district_id",
        "district_name": f"{series}_district_name",
    }
    for k, v in need.items():
        if v not in df.columns:
            raise ValueError(f"Missing required column: {v}")
    for c in ["has_pdf","has_eb_pages","llm_csv","has_eb_rows"]:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")
    return {
        **need,
        "filename": "filename" if "filename" in df.columns else None,
    }

def ensure_binary(df, cols):
    for c in cols:
        df[c] = (df[c].fillna(0).astype(float) > 0).astype(int)

def pct(n, d): return "0.00%" if d == 0 else f"{100*n/d:.2f}%"

def funnel_counts(df, stages):
    total = len(df)
    keeps = []
    kept = df
    for i, c in enumerate(stages):
        kept = kept[kept[c] == 1] if i else df[df[c] == 1]
        keeps.append(len(kept))
    drops = [total - keeps[0]] + [keeps[i-1] - keeps[i] for i in range(1, len(keeps))]
    return total, keeps, drops

# ---- main ----
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])
    ap.add_argument("--in_dir", default=".", help="Directory containing <series>_handbook_processing_loss.dta")
    ap.add_argument("--out_dir", default=".", help="Directory to write <series>_hb_processing_report.md")
    args = ap.parse_args()

    in_path  = Path(args.in_dir)  / f"{args.series}_handbook_processing_loss.dta"
    out_path = Path(args.out_dir) / f"{args.series}_hb_processing_report.md"

    df = pd.read_stata(in_path)
    cols = detect_columns(df, args.series)
    stages = ["has_pdf","has_eb_pages","llm_csv","has_eb_rows"]
    ensure_binary(df, stages)

    total, keeps, drops = funnel_counts(df, stages)
    s_id, s_name = cols["state_id"], cols["state_name"]
    d_id, d_name = cols["district_id"], cols["district_name"]
    fn = cols["filename"]

    # Overall failures
    fails = {
        "Missing PDF": df[df.has_pdf.eq(0)],
        "No EB pages (given PDF)": df[df.has_pdf.eq(1) & df.has_eb_pages.eq(0)],
        "No CSV (given EB pages)": df[df.has_pdf.eq(1) & df.has_eb_pages.eq(1) & df.llm_csv.eq(0)],
        "No reliable EB rows (given CSV)": df[df.has_pdf.eq(1) & df.has_eb_pages.eq(1) & df.llm_csv.eq(1) & df.has_eb_rows.eq(0)],
    }

    # State funnels
    states = df[[s_id, s_name]].drop_duplicates().sort_values([s_id])

    lines = []
    lines += [f"# {args.series.upper()} Handbook Processing — Data Loss Report",
              f"_Input_: `{in_path}`  ",
              f"_Output_: `{out_path}`", ""]

    lines += ["## Overall Attrition Funnel",
              "| Stage | Kept | % of Total | Dropped from Prev |",
              "|---|---:|---:|---:|"]
    labels = ["PDF present","EB pages found","CSV extracted","Reliable EB rows"]
    prev = total
    for i, lab in enumerate(labels):
        kept = keeps[i]
        lines.append(f"| {i+1}. {lab} | {kept} | {pct(kept,total)} | {prev - kept} |")
        prev = kept
    lines.append("")
    lines += ["### Drop-offs by Stage",
              "| Between Stages | Dropped |",
              "|---|---:|",
              f"| total → PDF present | {drops[0]} |",
              f"| PDF present → EB pages found | {drops[1]} |",
              f"| EB pages found → CSV extracted | {drops[2]} |",
              f"| CSV extracted → Reliable EB rows | {drops[3]} |",
              ""]

    lines.append("## Failure Drill-down (overall)")
    for title, df_fail in fails.items():
        lines.append(f"### {title}")
        if df_fail.empty:
            lines.append("_None_\n"); continue
        cols_show = [s_id, s_name, d_id, d_name] + ([fn] if fn else [])
        lines.append("| " + " | ".join(cols_show) + " |")
        lines.append("|" + "|".join(["---"]*len(cols_show)) + "|")
        for _, r in df_fail.sort_values([s_id, d_id]).iterrows():
            lines.append("| " + " | ".join(str(r[c]) for c in cols_show) + " |")
        lines.append("")

    lines += ["## State-level Funnels",
              "| State ID | State | Total | PDF kept | EB pages kept | CSV kept | Reliable rows kept |",
              "|---:|---|---:|---:|---:|---:|---:|"]
    for _, st in states.iterrows():
        sid, sname = st[s_id], st[s_name]
        sdf = df[df[s_id] == sid]
        st_total, st_keeps, _ = funnel_counts(sdf, stages)
        lines.append(
            f"| {sid} | {sname} | {st_total} | "
            f"{st_keeps[0]} ({pct(st_keeps[0],st_total)}) | "
            f"{st_keeps[1]} ({pct(st_keeps[1],st_total)}) | "
            f"{st_keeps[2]} ({pct(st_keeps[2],st_total)}) | "
            f"{st_keeps[3]} ({pct(st_keeps[3],st_total)}) |"
        )
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()

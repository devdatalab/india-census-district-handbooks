/* This do file merges the combined district handbook dataset with a pre-made key that maps each filename to district id and state id from pc01 town directory.
After merging, it assesses the coverage of this merged dataset against the urban PCA dataset to identify gaps and overlaps. */


local pdfdir = "$hb_pdf"
local csvdir = "`pdfdir'/eb_table_extracts"
local series "$hb_series"
if "`series'"=="" local series "pc01" // default if not set

/* Load the combined district handbook dataset */
import delimited using "`csvdir'/combined_hb/_file_manifest.csv", clear
rename source_file filename
recast str filename, force

/* keep only part of the filename before _EB suffix */
replace filename = regexr(filename, "_EB$", "")

/* Check for duplicates */
duplicates drop filename, force

/* Get unique filenames only */
levelsof filename, local(file_list) clean

/* Merge with district key - use m:1 since manifest has multiple rows per file */
merge m:1 filename using "$tmp/`series'_hb_pdf_key", generate(_merge_key)

list filename if _merge_key ==1

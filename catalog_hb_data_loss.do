/*
Merge handbook PDF filenames with pre-made PDF-district keys and compare coverage against urban PCA districts. Then track attrition through:
1) PDFs present vs. urban PCA districts
2) PDFs with identified EB pages
3) Those with CSV extracts
4) Those CSVs with reliable EB rows

Prereq: run config.do / hb_define_paths to populate:
$hb_series (pc01/pc11/pc91), $hb_pdf (PDF dir), $hb_extracts (extracts dir), $hb_eb_pages_csv (CSV of EB pages)
*/

clear
set more off

if "$hb_series" == "pc01" {
    global pcaroot "$pc01"
}
else if "$hb_series" == "pc11" {
    global pcaroot "$pc11"
}
else if "$hb_series" == "pc91" {
    global pcaroot "$pc91"
}


/*************************************************/
/* Stage 1: pdfs present vs. urban PCA districts */
/*************************************************/

/* Nonrecursive file listing of PDFs in $hb_pdf */
filelist, dir($hb_pdf) pattern("*.pdf") norecursive
save $tmp/${hb_series}_og_pdf_names.dta, replace

/* Include additional drop-in folder */
filelist, dir($hb_pdf/taha_2025_09_19) pattern("*.pdf") norecursive
save $tmp/${hb_series}_added_pdf_names.dta, replace

filelist, dir($hb_pdf/taha_2025_09_19) pattern("*.xls") norecursive

append using $tmp/${hb_series}_og_pdf_names.dta
append using $tmp/${hb_series}_added_pdf_names.dta

/* Keep unique filenames */
keep filename
sort filename
duplicates drop filename, force
save $tmp/${hb_series}_handbooks_list.dta, replace

/* ---------------------------- cell ---------------------------- */
/* Merge with PDF-district key; then with urban PCA districts to assess coverage */
use $tmp/${hb_series}_handbooks_list.dta, clear

/* Drop .pdf/.xls suffix; drop optional _EB suffix */
replace filename = substr(filename, 1, length(filename)-4)
replace filename = regexr(filename, "_EB$", "")
isid filename

/* Merge with prepared handbook–urban PCA key */
merge 1:1 filename using $tmp/${hb_series}_hb_pdf_key.dta, gen(hb_key_merge)

keep if hb_key_merge == 3
drop hb_key_merge

/* Ensure IDs are strings; trim whitespace */
tostring ${hb_series}_state_id, replace
tostring ${hb_series}_district_id, replace

replace ${hb_series}_state_id    = trim(${hb_series}_state_id)
replace ${hb_series}_district_id = trim(${hb_series}_district_id)

/* Standardize: states = 2 digits, districts = 2 digits */
gen long _st = real(${hb_series}_state_id)
replace ${hb_series}_state_id = string(_st, "%02.0f") if _st < .
drop _st

gen long _dist = real(${hb_series}_district_id)
replace ${hb_series}_district_id = string(_dist, "%02.0f") if _dist < .
drop _dist

duplicates drop ${hb_series}_state_id ${hb_series}_district_id, force
duplicates list ${hb_series}_state_id ${hb_series}_district_id

/* Save unique handbook districts */
save $tmp/${hb_series}_hb_districts.dta, replace

/* ---------------------------- cell ---------------------------- */

/* Group urban PCA to state–district level for coverage comparison */
use $pcaroot/${hb_series}u_pca_clean.dta, clear

keep ${hb_series}_state_name ${hb_series}_state_id ${hb_series}_district_name ${hb_series}_district_id
duplicates drop ${hb_series}_state_name ${hb_series}_state_id ${hb_series}_district_name ${hb_series}_district_id, force
sort ${hb_series}_state_id ${hb_series}_district_id

/* PCA<->HB merge */
merge 1:1 ${hb_series}_state_id ${hb_series}_district_id using $tmp/${hb_series}_hb_districts.dta, gen(hb_pca_merge)

preserve
/* hb_pca_merge: 1 = PCA only; 3 = PCA and HB (2 = HB not in PCA) */
keep if hb_pca_merge == 1 | hb_pca_merge == 3
gen has_pdf = (hb_pca_merge == 3)
keep ${hb_series}_state_name ${hb_series}_state_id ${hb_series}_district_name ${hb_series}_district_id filename has_pdf
save $tmp/${hb_series}_data_loss_pdf.dta, replace
restore

/* PCA districts currently missing in HB */
preserve
keep if hb_pca_merge == 1
sort ${hb_series}_state_id ${hb_series}_district_id
save $tmp/${hb_series}_unmatched_dist_in_pca.dta, replace
restore

/* HB districts not in urban PCA (diagnostics) */
preserve
keep if hb_pca_merge == 2
sort ${hb_series}_state_id ${hb_series}_district_id
save $tmp/${hb_series}_unmatched_districts_in_hb.dta, replace
restore

/* Coverage among urban PCA districts */
quietly count if hb_pca_merge == 3
local covered = r(N)
quietly count
local total = r(N)
local covpct = 100 * `covered' / `total'
di as txt "$hb_series Handbooks cover " as res %6.2f `covpct' as txt "% of urban PCA districts"
/* ---------------------------- cell ---------------------------- */

/**********************************************************/
/* Stage 2: Of handbooks, which have eb pages identified? */
/**********************************************************/
/* load output of find_eb_pages */
import delimited using $hb_pdf/${hb_series}_page_ranges_for_review.csv, clear //this has manual corrections appended as well plus taha files
sort filename

replace filename = subinstr(filename, ".pdf", "", .)
duplicates drop filename, force

replace filename = "kaithal" if filename == "kaithal_EB"

save $tmp/${hb_series}_eb_page_number.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/${hb_series}_data_loss_pdf.dta, clear

/*  merge in second stage coverage*/
merge m:1 filename using $tmp/${hb_series}_eb_page_number.dta, gen(hb_eb_merge)
list filename if hb_eb_merge == 2

keep if hb_eb_merge == 1 | hb_eb_merge == 3
/* make binary variable indicating coverage */
gen has_eb_pages = !missing(start_page, end_page)
drop start_page end_page hb_eb_merge

/* save stage two results (urban pca coverage + eb page detection) */
save $tmp/${hb_series}_data_loss_pdf_eb.dta, replace


/******************************************************/
/* Stage 3: Of EB-page PDFs, which have CSV extracts? */
/******************************************************/

/* load all file names from tables extracted */
filelist, dir($hb_extracts) pattern("*.csv") norecursive
keep filename
replace filename = subinstr(filename, "_EB.csv", "", .)
save $tmp/${hb_series}_csv_list.dta, replace

/* bring back stage 2 table and merge into stage 3 results */
use $tmp/${hb_series}_data_loss_pdf_eb.dta, clear
merge m:1 filename using $tmp/${hb_series}_csv_list.dta, gen(hb_csv_merge)

keep if hb_csv_merge == 1 | hb_csv_merge == 3
/* Rows with code 2 are filenames that exist in the CSV list but are not tied to a district in our Stage 2 table
and are not needed for the attrition tally. */

/* flag presence of an extract csv for this filename */
gen llm_csv = (hb_csv_merge == 3)
drop hb_csv_merge
save $tmp/${hb_series}_data_loss_pdf_eb_csv.dta, replace

/*******************************************************************************************************/
/* Stage 4: Of those CSVs, which have sufficient reliable EB rows?                                     */
/*******************************************************************************************************/
/* load output of combine_eb, this is eb level combined dataset */
import delimited using $hb_extracts/combined_hb/_file_manifest.csv, varnames(1) clear

/* Forward-fill town names only in unambiguous cases */
replace town_hb = town_hb[_n-1] if missing(town_hb) & _n>1 & _n<_N ///
    & !missing(town_hb[_n-1]) & town_hb[_n-1]==town_hb[_n+1]

/* Count “good” rows per file; keep one row per source_file */
gen has_data = 1 if !missing(town_hb, ward_name, eb_no, total_pop)
egen cnt_good_rows = total(has_data), by(source_file)
bysort source_file: keep if _n == 1

/* Diagnostic: list sparse files */
list source_file cnt_good_rows if cnt_good_rows < 10

/* Normalize to handbook basename; flag sufficient EB rows (>10) */
rename source_file filename
replace filename = subinstr(filename, "_EB", "", .)
gen has_eb_rows = (cnt_good_rows > 10)
keep filename has_eb_rows
save $tmp/${hb_series}_row_list.dta, replace

/* now merge in stage 4 results into report */
use $tmp/${hb_series}_data_loss_pdf_eb_csv.dta, clear
merge m:1 filename using $tmp/${hb_series}_row_list.dta, gen(hb_row_merge)

keep if hb_row_merge == 1 | hb_row_merge == 3
drop hb_row_merge
replace has_eb_rows = 0 if missing(has_eb_rows)

/* Final attrition table */
save $tmp/${hb_series}_handbook_processing_loss.dta, replace

exit


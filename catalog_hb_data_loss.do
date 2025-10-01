/* This do file merges handbook pdf filenames with pre-made pdf-district keys and compare coverage against urban pca districts,
then track attrition across the following stages:
1. PDFs present vs. urban PCA districts
2. PDFs that also have identified EB pages
3. Those with CSV extracts
4. Those CSVs that contain reliable EB rows

It assumes you've already run config.do and hb_define_paths to populate:
$hb_series (pc01/pc11/pc91), $hb_pdf (PDF dir), $hb_extracts (extracts dir), $hb_eb_pages_csv (CSV of EB pages)
*/

clear
set more off

/* Pull series/paths from config.do */
local series "$hb_series"
if "`series'"=="" local series "pc01"   // default if not set
local pdfdir "$hb_pdf"                  // directory containing district handbook PDFs
local extracts "$hb_extracts"           // .../eb_table_extracts directory containing extracted CSVs
local ebpages "$hb_eb_pages_csv"        // urban_eb_pages.csv path

/* Map series to PCA root macros if you already have $pc01/$pc11/$pc91 set elsewhere */
local pcaroot ""
if "`series'"=="pc01" local pcaroot "$pc01"
else if "`series'"=="pc11" local pcaroot "$pc11"
else if "`series'"=="pc91" local pcaroot "$pc91"

/************************************************************/
/* Stage 1 attrition: pdf file exists compared to urban pca */
/************************************************************/

/* get a list of pdfs, nonrecursive so we don't look in subdirectories */
filelist, dir("`pdfdir'") pattern("*.pdf") norecursive
/* keep only files whose names start with "DH_" for now */
keep if regexm(filename, "^DH_")

keep filename
sort filename
duplicates drop filename, force
/* save to tmp a list of pdfs */
save $tmp/`series'_pdf_names.dta, replace

/* ---------------------------- cell ---------------------------- */

/* Merge with pdf–district keys; merge with urban PCA districts to see coverage */
use $tmp/`series'_pdf_names.dta, clear

replace filename = substr(filename, 1, length(filename)-4)
isid filename
merge 1:1 filename using $tmp/`series'_hb_pdf_key.dta, gen(hb_key_merge)

/* rename variables with pcxx_ */
rename (state_*) (`series'_state_*)
rename (district_*) (`series'_district_*)

/* save a list of pdfs in pc and not in keys for update purposes */
preserve
keep if hb_key_merge == 1
sort filename
export delimited using $tmp/`series'_keys_to_update.csv, replace // if not empty, this means your keys need updating
restore

duplicates drop `series'_*, force
duplicates list `series'_state_id `series'_district_id

/* Clean whitespace in state and district id */
replace `series'_state_id = trim(`series'_state_id)
replace `series'_district_id = trim(`series'_district_id)

/* Standardize state IDs to two digits */
gen long _st = real(`series'_state_id)
replace `series'_state_id  = string(_st, "%02.0f") if _st < .
drop _st
gen long _dist = real(`series'_district_id)
replace `series'_district_id = string(_dist, "%02.0f") if _dist < .
drop _dist

/* save in tmp a list of unique handbook districts */
save $tmp/`series'_hb_districts.dta, replace

/* group urban pca to the state–district level for coverage comparison */
use "`pcaroot'/`series'u_pca_clean.dta", clear
keep `series'_state_name `series'_state_id `series'_district_name `series'_district_id
duplicates drop `series'_state_name `series'_state_id `series'_district_name `series'_district_id, force

sort `series'_state_id `series'_district_id

/* urban pca-hb pdf merge */
merge 1:1 `series'_state_id `series'_district_id using $tmp/`series'_hb_districts.dta, gen(hb_pca_merge)
/* create a binary variable indicating whether we have district handbook for a given pca urban district */
preserve
/* hb_pca_merge==1 is urban pca only, hb_pca_merge==3 is urban pca with handbook coverage */
keep if hb_pca_merge == 1 | hb_pca_merge == 3 // note that hb_pca_merge==2 are handbook districts not in pca urban
gen has_pdf = hb_pca_merge == 3

keep `series'_state_name `series'_state_id `series'_district_name `series'_district_id filename has_pdf

/* save in tmp initial table documenting handbook coverage for urban pca */
save $tmp/`series'_data_loss_pdf.dta, replace
restore

/* save a list of districts in urban pca that we currently don't have in directory */
preserve
keep if hb_pca_merge == 1
sort `series'_state_id `series'_district_id
save $tmp/`series'_umatched_dist_in_pca.dta, replace
restore

/* save a list of districts in hb that we currently don't have in hb, this is for error checking */
preserve
keep if hb_pca_merge == 2
sort `series'_state_id `series'_district_id
save $tmp/`series'_unmactheddistricts_in_hb.dta, replace
restore

/* calculate coverage among pca urban districts */
qui count if hb_pca_merge == 3
local covered = r(N)

qui count
local total = r(N)

local covpct = 100 * `covered' / `total'
di as txt "`series' Handbooks cover " as res %6.2f `covpct' "% of pca urban districts"


/********************************************************************************/
/* Stage 2 attrition: out of handbook pdfs, which ones have eb pages identified */
/********************************************************************************/
/* import ebpages which is the csv with page ranges identified by find_eb_pages */
import delimited "`ebpages'", varnames(nonames) clear

rename v1 filename
rename v2 page_number

sort filename
replace filename = subinstr(filename, ".pdf", "", .)
/* let each row be a unique filename that has at least one eb page number */
duplicates drop filename, force

save $tmp/`series'_eb_page_number, replace

/* ---------------------------- cell ---------------------------- */
/* load data loss table and create new binary documenting find_eb_pages results */
use $tmp/`series'_data_loss_pdf.dta, clear

/* use m:1 merge because master is district level; using has unique filename */
merge m:1 filename using $tmp/`series'_eb_page_number, gen(hb_eb_merge)

keep if hb_eb_merge == 1 | hb_eb_merge == 3

/* get a binary = 1 if that filename has at least one page_number corresponding to it */
gen has_eb_pages = !missing(page_number)
drop page_number hb_eb_merge

/* save in tmp table with attrition at two stages: pdf coverage of pca districts, eb table pages found */
save $tmp/`series'_data_loss_pdf_eb, replace


/*****************************************************************************************/
/* Stage 3 attrition: out of pdfs with eb pages identified, which ones converted to csvs */
/*****************************************************************************************/

/* get a list of csvs in eb_table_extracts */
filelist, dir("`extracts'") pattern("*.csv") norecursive

keep filename
replace filename = subinstr(filename, "_EB.csv", "", .)

/* save extracted csv list */
save $tmp/`series'_csv_list.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/`series'_data_loss_pdf_eb, clear

/* using m:1 because data_loss table is at urban pca level and can contain multiple rows of empty filname */
merge m:1 filename using $tmp/`series'_csv_list, gen(hb_csv_merge)


keep if hb_csv_merge == 1 | hb_csv_merge == 3 // note that 2 is hb pdfs not in urban pca

gen llm_csv = hb_csv_merge == 3
drop hb_csv_merge

/* save in tmp table with attrition at three stages: pdf coverage of pca districts, eb table pages found, llm extracted csvs */
save $tmp/`series'_data_loss_pdf_eb_csv, replace


/*******************************************************************************************************/
/* Stage 4 attrition: out of pdfs with eb pages and converted to csvs, which ones has reliable eb rows */
/*******************************************************************************************************/

/* load combined handbook data, this has pca districts merged in as well, see $hb_code/combine_eb_tables.py */
import delimited using "`extracts'/combined_hb/_file_manifest.csv", varnames(1) clear

/* fill rows that have empty town names, carry-forward town names only in unambiguous cases */
replace town_hb = town_hb[_n-1] if missing(town_hb) & _n>1 & _n<_N ///
    & !missing(town_hb[_n-1]) & town_hb[_n-1]==town_hb[_n+1]

/* count for each filename (called source_file here), rows containing legible fields */
gen has_data = 1 if !missing(town_hb, ward_name, eb_no, total_pop)
egen cnt_good_rows = total(has_data), by(source_file)

/* keep only one observation per source_file */
bysort source_file: keep if _n == 1

/* list source_file that has less than 20 rows of legible data */
list source_file cnt_good_rows if cnt_good_rows < 20

rename source_file filename
replace filename = subinstr(filename, "_EB", "", .)

/* create a variable set to one if filename has more than 10 rows of good data */
gen has_eb_rows = 0
replace has_eb_rows = 1 if cnt_good_rows > 10

keep filename has_eb_rows

save $tmp/`series'_row_list, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/`series'_data_loss_pdf_eb_csv, clear

merge m:1 filename using $tmp/`series'_row_list, gen(hb_row_merge)

/* xn: investigate why merge==2 drop by 2 */
keep if hb_row_merge == 1 | hb_row_merge == 3
drop hb_row_merge
replace has_eb_rows = 0 if missing(has_eb_rows)

/* output final attrition table */
save $tmp/`series'_handbook_processing_loss.dta, replace

exit

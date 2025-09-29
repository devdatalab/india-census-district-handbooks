/* This do file merge handbook pdf filenames with pre-made pdf-district keys and compare coverage against urban pca districts for both pc01 and pc91.*/
/* Input:
$scode/b/handbooks/hb_processing/pc01_hb_pdf_keys.csv
$scode/b/handbooks/hb_processing/pc91_hb_pdf_keys.csv for pdf-district keys
$pc01/pc01u_pca_clean.dta
$pc91/pc91u_pca_clean.dta for urban pca districts

Produces:
list of pca urban districts without a mapped handbook.
*/

clear 
set more off

/************************************************************/
/* Stage 1 attrition: pdf file exists compared to urban pca */
/************************************************************/

/* get a list of pc01 pdfs, use nonrecursive so that we are not looking within subdirectory */
filelist, dir("$pc01/district_handbooks") pattern("*.pdf") norecursive
/* keep only files whose names start with "DH_" */
keep if regexm(filename, "^DH_")

keep filename
sort filename
duplicates drop filename, force
save $tmp/pdf_names_pc01.dta, replace


/* /\* get a list of pc91 pdfs *\/ */
/* filelist, dir("$pc91/district_handbooks") pattern("*.pdf") norecursive */
/* keep filename */
/* sort filename */
/* duplicates drop filename, force */
/* save $tmp/pdf_names_pc91.dta, replace */


/* /\* get a list of pc11 pdfs *\/ */
/* filelist, dir("$pc11/district_handbooks_xii_b") pattern("*.pdf") */
/* keep filename */
/* sort filename */
/* duplicates drop filename, force */
/* save $tmp/pdf_names_pc11.dta, replace */


/* Merge with pdf–district keys; merge with urban PCA districts to see coverage */ 
/* open the list of pdfs in this year */
use $tmp/pdf_names_pc01.dta, clear

replace filename = substr(filename, 1, length(filename)-4)
isid filename
merge 1:1 filename using $tmp/pc01_hb_pdf_key.dta, gen(hb_key_merge)

rename state_* pc01_state_*
rename district_* pc01_district_*

/* save a list of pdfs in pc and not in keys for update purposes */
preserve
keep if hb_key_merge == 1
sort filename
export delimited using $tmp/pc01_keys_to_update.csv, replace
restore

duplicates drop pc01_*, force
duplicates list pc01_state_id pc01_district_id

/* Clean whitespace in state and district id */
replace pc01_state_id = trim(pc01_state_id)
replace pc01_district_id = trim(pc01_district_id)

/* Standardize state IDs to a two-digit format (e.g., '9' becomes '09') for consistent merging */
gen long _st = real(pc01_state_id)
replace pc01_state_id  = string(_st, "%02.0f") if _st < .
drop _st
gen long _dist = real(pc01_district_id)
replace pc01_district_id = string(_dist, "%02.0f") if _dist < .
drop _dist

save $tmp/pc01_hb_districts.dta, replace

/* group urban pca to the state–district level for coverage comparison */
use $pc01/pc01u_pca_clean.dta, clear
keep pc01_state_name pc01_state_id pc01_district_name pc01_district_id
duplicates drop pc01_state_name pc01_state_id pc01_district_name pc01_district_id, force

sort pc01_state_id pc01_district_id 

/* urban pca-hb pdf merge */
merge 1:1 pc01_state_id pc01_district_id using $tmp/pc01_hb_districts.dta, gen(hb_pca_merge)
/* create a binar variable indicating whether we have district handbook for a given pca urban district */
preserve
keep if hb_pca_merge == 1 | hb_pca_merge == 3 // note that hb_pca_merge==2 are handbook districts not in pca urban
gen has_pdf = hb_pca_merge == 3

keep pc01_state_name pc01_state_id pc01_district_name pc01_district_id filename has_pdf

save $tmp/pc01_data_loss_pdf.dta, replace
restore

/* save a list of districts in urban pca that we currently don't have in directory */
preserve
keep if hb_pca_merge == 1
sort pc01_state_id pc01_district_id
save $tmp/pc01_umatched_dist_in_pca.dta, replace
restore

/* save a list of districts in hb that we currently don't have in hb, this is for error checking */
preserve
keep if hb_pca_merge == 2
sort pc01_state_id pc01_district_id
save $tmp/pc01_unmactheddistricts_in_hb.dta, replace
restore

/* calculate coverage among pca urban districts */
qui count if hb_pca_merge == 3
local covered = r(N)

qui count
local total = r(N)

local covpct = 100 * `covered' / `total'
di as txt "pc01 Handbooks cover " as res %6.2f `covpct' "% of pca urban districts"



/********************************************************************************/
/* Stage 2 attrition: out of handbook pdfs, which ones have eb pages identified */
/********************************************************************************/
import delimited $pc01/district_handbooks/urban_eb_pages.csv, varnames(nonames) clear

rename v1 filename
rename v2 page_number

sort filename
replace filename = subinstr(filename, ".pdf", "", .)
/* let each row be a unique filename that has at least one eb page number */
duplicates drop filename, force

save $tmp/pc01_eb_page_number, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/pc01_data_loss_pdf.dta, clear

/* Use m:1 merge because the master dataset is at the district level, which means a single filename can correspond to multiple observations (e.g., empty filenames for districts without a handbook).
The using dataset has a unique filename for each entry. */
merge m:1 filename using $tmp/pc01_eb_page_number, gen(hb_eb_merge)

keep if hb_eb_merge == 1 | hb_eb_merge == 3

/* get a binary = 1 if that filename has at least one page_number corresponding to it */
gen has_eb_pages = !missing(page_number)
drop page_number hb_eb_merge

save $tmp/pc01_data_loss_pdf_eb, replace



/*****************************************************************************************/
/* Stage 3 attrition: out of pdfs with eb pages identified, which ones converted to csvs */
/*****************************************************************************************/

/* get a list of csvs in $pc01/district_handbooks/eb_table_extracts */
filelist, dir($pc01/district_handbooks/eb_table_extracts) pattern("*.csv") norecursive

keep filename
replace filename = subinstr(filename, "_EB.csv", "", .)
save $tmp/pc01_csv_list.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/pc01_data_loss_pdf_eb, clear

merge m:1 filename using $tmp/pc01_csv_list, gen(hb_csv_merge)

keep if hb_csv_merge == 1 | hb_csv_merge == 3 //note that 2 is hb pdfs not in urban pca

gen llm_csv = hb_csv_merge == 3
drop hb_csv_merge

save $tmp/pc01_data_loss_pdf_eb_csv, replace


/*******************************************************************************************************/
/* Stage 4 attrition: out of pdfs with eb pages and converted to csvs, which ones has reliable eb rows */
/*******************************************************************************************************/

import delimited using $pc01/district_handbooks/eb_table_extracts/combined_hb/_file_manifest.csv, varnames(1) clear


/* Some handbook pages leave blank town rows (name not carried down).
Fill town_hb only when the immediate neighbors above and below are both non-missing and identical. Requires original row order; does not fill ambiguous gaps. */
replace town_hb = town_hb[_n-1] if missing(town_hb) & _n>1 & _n<_N ///
    & !missing(town_hb[_n-1]) & town_hb[_n-1]==town_hb[_n+1]


/* count for each filename (called source_file here), there are ? number of rows containing legible town_hb ward_number eb_no total_pop */
gen has_data = 1 if !missing(town_hb, ward_name, eb_no, total_pop)
egen cnt_good_rows = total(has_data), by(source_file)

/* keep only one observation per source_file */
bysort source_file: keep if _n == 1

/* list source_file that has less than 10 rows of legible data, investigate individually against pdfs */
list source_file cnt_good_rows if cnt_good_rows < 20

rename source_file filename
replace filename = subinstr(filename, "_EB", "", .)

/* create a variable set to one if filename has more than 10 rows of good data */
gen has_eb_rows = cnt_good_rows > 10

keep filename cnt_good_rows has_eb_rows

save $tmp/pc01_row_list, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/pc01_data_loss_pdf_eb_csv, clear

merge m:1 filename using $tmp/pc01_row_list, gen(hb_row_merge)

/* xn: investigate why merge==2 drop by 2 */
keep if hb_row_merge == 1 | hb_row_merge == 3
drop hb_row_merge

exit

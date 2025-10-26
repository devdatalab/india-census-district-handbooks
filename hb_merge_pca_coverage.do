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

save $tmp/`series'_combined_hb.dta, replace

/* Check for duplicates */
duplicates drop filename, force

/* Get unique filenames only */
levelsof filename, local(file_list) clean

/* Merge with district key - use m:1 since manifest has multiple rows per file */
merge m:1 filename using "$tmp/`series'_hb_pdf_key", gen(_merge_key)
keep if _merge_key == 3
drop _merge_key

keep filename `series'_state_id `series'_district_id `series'_state_name `series'_district_name
destring `series'_state_id `series'_district_id, replace

/* save state district level with key */
save $tmp/`series'_hb_uniq_state_dist_w_key.dta, replace

/* merge back into combined handbooks */
merge 1:m filename using $tmp/`series'_combined_hb.dta

drop _merge
rename town_hb `series'_town_hb

foreach v in `series'_state_name `series'_district_name `series'_town_hb {
  name_clean `v', replace
}

save $tmp/`series'_combined_hb_w_key.dta, replace

/* ---------------------------- cell ---------------------------- */
/* merge with pca towns to assess average */
local series pc01
use ${`series'}/`series'u_pca_clean.dta, clear

destring `series'_state_id `series'_district_id, replace

/* count number of districts with at least one town >= 1000 people  */
gen town1000 = pc01_pca_tot_p >= 1000 if !missing(pc01_pca_tot_p)

* district-level flag: does this district have any such town?
bys pc01_state_id pc01_district_id: egen has_big_town = max(town1000)

* count distinct districts with at least one big town
egen tag = tag(pc01_state_id pc01_district_id)
count if tag & has_big_town
di as result "PCA districts with ≥1 town ≥1,000 people: " r(N)

/* keep only big towns */
keep if tag & has_big_town

merge m:1 `series'_state_id `series'_district_id using $tmp/`series'_hb_uniq_state_dist_w_key, gen(_pca_merge)

/* calculate coverage stats */
count if _pca_merge == 3
local matched = r(N)
count
local total = r(N)
local pct = (`matched' / `total')*100

di as result "`matched' out of `total' (" %3.1f `pct' "%) of PCA large urban districts are covered by district handbooks"

/* keep only districts that are covered by handbooks */
keep if _pca_merge == 3
drop _pca_merge

rename `series'_town_name `series'_town_pca

keep `series'_state_name `series'_state_id `series'_district_name `series'_district_id `series'_town_id `series'_town_pca

foreach v in `series'_state_name `series'_district_name `series'_town_pca {
  name_clean `v', replace
}

/* create a shared std town variable for masala merge */
gen std_town = `series'_town_pca

/* save lhs: this is the urban pca districts with at least one big town and also covered by district handbooks */
save $tmp/`series'u_town_pca_df, replace

/* ---------------------------- cell ---------------------------- */
/* laod rhs */
local series pc01
use $tmp/`series'_combined_hb_w_key.dta, clear

count if missing(`series'_town_hb)

/* Some handbook pages leave blank town rows (name not carried down).
Fill town_hb only when the immediate neighbors above and below are both non-missing and identical. Requires original row order; does not fill ambiguous gaps. */
replace `series'_town_hb = `series'_town_hb[_n-1] if missing(`series'_town_hb) & _n>1 & _n<_N ///
    & !missing(`series'_town_hb[_n-1]) & `series'_town_hb[_n-1]==`series'_town_hb[_n+1]


/* collapse to town-level */
keep filename `series'_state_id `series'_state_name `series'_district_id `series'_district_name `series'_town_hb

duplicates drop

/* make unified var name */
gen std_town = `series'_town_hb

save $tmp/`series'_town_hb_df, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/`series'u_town_pca_df, clear

/* masala merge with district_handbook towns within each state and district */
masala_merge `series'_state_id `series'_district_id using $tmp/`series'_town_hb_df, s1(std_town) ///
    method(both) keepambiguous fuzziness(3.0)


exit
/* ---------------------------- cell ---------------------------- */
/* merge with shrid */
use ~/iec/frozen_data/shrug/v2.1.pakora/pc-keys/native/`series'u_shrid_key.dta, clear

recast str `series'_state_id `series'_district_id, force

duplicates drop `series'_state_id `series'_district_id, force

save $tmp/`series'u_shrid_key.dta, replace
/* ---------------------------- cell ---------------------------- */

use $tmp/`series'_combined_handbooks.dta, clear

merge m:1 `series'_state_id `series'_district_id using $tmp/`series'u_shrid_key.dta, gen(shrid_merge)

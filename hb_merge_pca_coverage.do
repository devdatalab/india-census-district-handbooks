/* This do file merges the combined district handbook dataset with a pre-made key that maps each filename to district id and state id from pc01 town directory.
After merging, it assesses the coverage of this merged dataset against the urban PCA dataset to identify gaps and overlaps. */


local pdfdir = "$hb_pdf"
local csvdir = "`pdfdir'/eb_table_extracts"
global series "$hb_series"
if "$series"=="" global series "pc01" // default if not set

/* Load the combined district handbook dataset */
import delimited using "`csvdir'/combined_hb/_file_manifest.csv", clear

rename source_file filename
recast str filename, force

/* keep only part of the filename before _EB suffix */
replace filename = regexr(filename, "_EB$", "")

/* clean var names */
name_clean town_hb, replace

save $tmp/${series}_combined_hb.dta, replace

/* Check for duplicates */
duplicates drop filename, force

/* Get unique filenames only */
levelsof filename, local(file_list) clean

/* Merge with district key - use m:1 since manifest has multiple rows per file */
merge m:1 filename using "$tmp/${series}_hb_pdf_key", gen(_merge_key)
keep if _merge_key == 3
drop _merge_key

keep filename ${series}_state_id ${series}_district_id ${series}_state_name ${series}_district_name
destring ${series}_state_id ${series}_district_id, replace

/* save state district level with key */
save $tmp/${series}_hb_uniq_state_dist_w_key.dta, replace

/* merge back into combined handbooks */
merge 1:m filename using $tmp/${series}_combined_hb.dta

drop _merge
rename town_hb ${series}_town_hb

/* save eb level district handbook data */
save $tmp/${series}_combined_hb_w_key.dta, replace

/* ---------------------------- cell ---------------------------- */
/* load urban pca */
use ${${series}}/pc01u_pca_clean.dta, clear

destring ${series}_state_id ${series}_district_id, replace

/* clean variable names */
name_clean ${series}_state_name, replace
name_clean ${series}_district_name, replace
name_clean ${series}_town_name, replace

save $tmp/${series}u_pca_clean.dta, replace

/* count number of districts with at least one town >= 1000 people  */
gen town1000 = pc01_pca_tot_p >= 1000 if !missing(pc01_pca_tot_p)

* district-level flag: does this district have any such town?
bys pc01_state_id pc01_district_id: egen has_big_town = max(town1000)

* count distinct districts with at least one big town
egen tag = tag(pc01_state_id pc01_district_id)
count if tag & has_big_town
di as result "PCA districts with ≥1 town ≥1,000 people: " r(N)

/* keep only districts with at least one big town */
keep if tag & has_big_town

/* keep only location vars */
keep ${series}_state_name ${series}_state_id ${series}_district_name ${series}_district_id
duplicates drop

/* merge with state-district level handbooks to assess coverage */
merge 1:m ${series}_state_id ${series}_district_id using $tmp/${series}_hb_uniq_state_dist_w_key.dta, gen(_pca_merge)

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

/* now merge back into pca town lists to prep for fuzzy match */
merge 1:m ${series}_state_id ${series}_district_id using $tmp/${series}u_pca_clean.dta, gen(_town_merge)

keep if _town_merge
drop _town_merge

keep ${series}_state_name ${series}_state_id ${series}_district_name ${series}_district_id ${series}_town_id ${series}_town_name
rename ${series}_town_name ${series}_town_pca

/* create a shared std town variable for masala merge */
gen std_town = ${series}_town_pca

duplicates drop
duplicates drop ${series}_state_id ${series}_district_id ${series}_town_pca, force // 9,20,faridpur and 27,9,andri ct repeated twice?

/* save lhs: this is the urban pca districts with at least one big town and also covered by district handbooks */
save $tmp/${series}u_town_pca_df, replace

/* ---------------------------- cell ---------------------------- */
/* laod rhs */
use $tmp/${series}_combined_hb_w_key.dta, clear

count if missing(${series}_town_hb)

/* Some handbook pages leave blank town rows (name not carried down).
Fill town_hb only when the immediate neighbors above and below are both non-missing and identical. Requires original row order; does not fill ambiguous gaps. */
replace ${series}_town_hb = ${series}_town_hb[_n-1] if missing(${series}_town_hb) & _n>1 & _n<_N ///
    & !missing(${series}_town_hb[_n-1]) & ${series}_town_hb[_n-1]==${series}_town_hb[_n+1]


/* collapse to town-level */
keep ${series}_state_name ${series}_district_name ${series}_state_id ${series}_district_id ${series}_town_hb

duplicates drop

/* make unified var name */
gen std_town = ${series}_town_hb

save $tmp/${series}_town_hb_df, replace

/* masala merge with district_handbook towns within each state and district */
masala_merge ${series}_state_id ${series}_district_id using $tmp/${series}u_town_pca_df, s1(std_town) method(both) fuzziness(3.5)

/* using here needs to be all matched */
/* keep if match_source == 7 */
/* export delimited using $tmp/unmatched_using.csv */
keep if match_source <= 4

/* ---------------------------- cell ---------------------------- */
/* merge back to to get eb-level dataset  */
merge 1:m ${series}_state_id ${series}_district_id ${series}_town_hb using $tmp/${series}_combined_hb_w_key.dta, gen(_eb_merge)

keep if _eb_merge == 3
drop _eb_merge


keep ${series}_state_id ${series}_state_name ${series}_district_name ${series}_district_id ${series}_town_hb ${series}_town_id ${series}_town_pca ///
    filename location_code ward_name eb_no total_pop sc_pop st_pop

save $tmp/${series}_combined_hb_w_pca_cln, replace

/* ---------------------------- cell ---------------------------- */
/* merge with shrid */
/* clean shrid 2 */
use ~/iec/frozen_data/shrug/v2.1.pakora/pc-keys/native/${series}u_shrid_key.dta, clear

destring ${series}_state_id ${series}_district_id, replace

duplicates drop ${series}_state_id ${series}_district_id ${series}_town_id, force // this only works if not multiple shrids map to the same town, make sure this is the case

save $tmp/${series}u_shrid_key.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/${series}_combined_hb_w_pca_cln, clear

/* set missings to zero -- these were marked as "-" in the handbook */
foreach v in total_pop sc_pop st_pop {
  replace `v' = 0 if mi(`v')
}


merge m:1 ${series}_state_id ${series}_district_id ${series}_town_id using $tmp/${series}u_shrid_key.dta, gen(_shrid_merge)

keep if _shrid_merge == 3

drop _shrid_merge

/* drop outlier enumeration blocks */
keep if inrange(total_pop, 1, 1250) & inrange(sc_pop, 0, 1250)

/* Majority = non-SC */
gen non_sc_pop = total_pop - sc_pop

/* eb count per shrid */
bys shrid: gen int eb_units_shrid = _N

/* create dissimilarity and isolation indices */
gen_dissimilarity, min(sc_pop) maj(non_sc_pop) gen("d_sc_${series}")  label("SC (shrid)") upper(shrid)
gen_isolation, min(sc_pop) maj(non_sc_pop) gen("iso_sc_${series}") label("SC (shrid)") upper(shrid)

replace d_sc_${series} = . if eb_units_shrid < 4
replace iso_sc_${series} = . if eb_units_shrid < 4 // is 4 a reasonable number here?

/* get population at the shrid lvl */
bys shrid: egen tot_pop_shrid_${series} = total(total_pop)
bys shrid: egen sc_pop_shrid_${series} = total(sc_pop)

/* collapse to shrid level: here taking the mean doesn't matter because populations and indices constant within shrid */
collapse (mean) d_sc_${series} iso_sc_${series} tot_pop_shrid_${series} sc_pop_shrid_${series} (firstnm) ${series}_state_id, by(shrid)

save $tmp/${series}_seg_sc_by_shrid.dta, replace

/* This do file merges the combined district handbook dataset with a pre-made key that maps each filename to district id and state id from pca town directory (e.g., pc01u_pca_clean).
After merging, it assesses the coverage of this merged dataset against the urban PCA dataset to identify gaps and overlaps. */
/* Fuzzy merge with urban pca towns so that we get shrid id tacked on; then merge with shrid v2 and compute segregation measures. */

local pdfdir = "$hb_pdf"
local csvdir = "`pdfdir'/eb_table_extracts"
global series "$hb_series"
if "$series"=="" global series "pc01" // default if not set

/* Load the combined district handbook dataset */
import delimited using "`csvdir'/combined_hb/_file_manifest.csv", clear

rename source_file filename
recast str filename, force

/* keep only the part of filename before _EB suffix to match pattern of key */
replace filename = regexr(filename, "_EB$", "")

/* clean var name */
name_clean town_hb, replace

save $tmp/${series}_combined_hb.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/${series}_combined_hb.dta, clear

/* Check for duplicates */
duplicates drop filename, force

/* Get unique filenames only */
qui levelsof filename, local(file_list) clean

/* Merge with district key use m:1 since manifest has multiple rows per file */
merge m:1 filename using $tmp/${series}_hb_pdf_key, gen(_merge_key)
// =2, =3 only otherwise update data/pc01_hb_pdf_key

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
use ${${series}}/${series}u_pca_clean.dta, clear

destring ${series}_state_id ${series}_district_id, replace

/* clean variable names */
name_clean ${series}_state_name, replace
name_clean ${series}_district_name, replace
name_clean ${series}_town_name, replace

save $tmp/${series}u_pca_clean.dta, replace

/* count number of districts with at least one town >= 1000 people  */
gen town1000 = ${series}_pca_tot_p >= 1000 if !missing(${series}_pca_tot_p)

* district-level flag: does this district have any such town?
bys ${series}_state_id ${series}_district_id: egen has_big_town = max(town1000)

* count distinct districts with at least one big town
egen tag = tag(${series}_state_id ${series}_district_id)
count if tag & has_big_town
di as result "PCA districts with ≥1 town ≥1,000 people: " r(N)

/* keep only districts with at least one big town */
// keep if tag & has_big_town

/* keep only location vars */
keep ${series}_state_name ${series}_state_id ${series}_district_name ${series}_district_id
duplicates drop

/* merge with state-district level handbooks to assess coverage of pca districts */
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


/* now merge back into pca town lists to prep for fuzzy match 1:m since master is district level */
merge 1:m ${series}_state_id ${series}_district_id using $tmp/${series}u_pca_clean.dta, gen(_town_merge)

keep if _town_merge == 3
drop _town_merge

keep ${series}_state_name ${series}_state_id ${series}_district_name ${series}_district_id ${series}_town_id ${series}_town_name
rename ${series}_town_name ${series}_town_pca

/* create a shared std town variable for masala merge */
gen std_town = ${series}_town_pca

duplicates list

duplicates drop ${series}_state_id ${series}_district_id ${series}_town_pca, force // 9,20,faridpur and 27,9,andri ct repeated twice? investigate

//count distinct towns at this stage
/* create a var that's concatenated with comma separator */
/* egen str town_identifier = ///
    concat(${series}_state_id ${series}_district_id ${series}_town_id), ///
    punct(", ") format(%12.0f)

distinct town_identifier*/

/* save rhs: this is the urban pca districts with at least one big town and also covered by district handbooks */
save $tmp/${series}u_town_pca_df, replace

/* ---------------------------- cell ---------------------------- */
/* laod lhs */
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

/* count number of unique towns at this stage, i.e., universe of handbook panel */
/* create a var that's concatenated with comma separator */
egen str town_identifier = ///
    concat(${series}_state_id ${series}_district_id ${series}_town_hb), ///
    punct(", ") format(%12.0f)

distinct town_identifier //this is the first stat for the attrition table


save $tmp/${series}_town_hb_df, replace

/* masala merge with district_handbook towns within each state and district */
masala_merge ${series}_state_id ${series}_district_id using $tmp/${series}u_town_pca_df, s1(std_town) method(both) fuzziness(3.5)

/* using here should be all matched */
/* keep if match_source == 7 */
/* export delimited using $tmp/unmatched_using.csv */
keep if match_source <= 4

// count unique number of towns preserved at this stage


save $tmp/${series}_towns_after_pca_matched, replace

/* ---------------------------- cell ---------------------------- */
/* merge back to to get eb-level dataset  */
merge 1:m ${series}_state_id ${series}_district_id ${series}_town_hb using $tmp/${series}_combined_hb_w_key.dta, gen(_eb_merge)
// ideally, after manual corrections after masala merge, this should all be able to have corresponding towns in urban pca

keep if _eb_merge == 3
drop _eb_merge
// count unique number of towns at this stage
distinct idm // this is the second stat for the attrition table, i.e., how many handbooks towns we got after fuzzy merge

keep idm ${series}_state_id ${series}_state_name ${series}_district_name ${series}_district_id ${series}_town_hb ${series}_town_id ${series}_town_pca ///
    filename location_code ward_name eb_no total_pop sc_pop st_pop

save $tmp/${series}_combined_hb_w_pca_cln, replace

/* ---------------------------- cell ---------------------------- */
/* merge with shrid */
/* clean shrid 2 */
use ~/iec/frozen_data/shrug/v2.1.pakora/pc-keys/native/${series}u_shrid_key.dta, clear

destring ${series}_state_id ${series}_district_id, replace

duplicates list ${series}_state_id ${series}_district_id ${series}_town_id shrid2

duplicates drop ${series}_state_id ${series}_district_id ${series}_town_id, force
// this only works if not multiple shrids map to the same town, make sure this is the case

save $tmp/${series}u_shrid_key.dta, replace

/* ---------------------------- cell ---------------------------- */
use $tmp/${series}_combined_hb_w_pca_cln, clear

/* set missings to zero these were marked as "-" in the handbook */
foreach v in total_pop sc_pop st_pop {
  replace `v' = 0 if mi(`v')
}

/* merge with shrid key, m:1 since multiple towns can match to same shrid */
merge m:1 ${series}_state_id ${series}_district_id ${series}_town_id using $tmp/${series}u_shrid_key.dta, gen(_shrid_merge)

keep if _shrid_merge == 3

save $tmp/${series}_combined_hb_w_pca_shrid_cln, replace

/* ---------------------------- cell ---------------------------- */

//distinct towns at this stage
distinct idm // this is the third stat of attrition table
distinct shrid2 // also report # number of shrids

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
replace iso_sc_${series} = . if eb_units_shrid < 4

/* get population at the shrid lvl */
bys shrid: egen tot_pop_shrid_${series} = total(total_pop)
bys shrid: egen sc_pop_shrid_${series} = total(sc_pop)

/* collapse to shrid level: here taking the mean doesn't matter because populations and indices constant within shrid */
collapse (mean) d_sc_${series} iso_sc_${series} tot_pop_shrid_${series} sc_pop_shrid_${series} (firstnm) ${series}_state_id ${series}_state_name, by(shrid)

save $tmp/${series}_seg_sc_by_shrid.dta, replace

/* ---------------------------- cell ---------------------------- */

/* merge the two years for time series */
use $tmp/pc01_seg_sc_by_shrid.dta, clear

merge 1:1 shrid2 using $tmp/pc11_seg_sc_by_shrid.dta, gen(_seg_shrid_merge)

keep if _seg_shrid_merge == 3

save $tmp/pc01_pc11_seg_shrid_cln.dta, replace

// distinct number of shrid2 at this stage
distinct shrid2
//this is the last number of attrition table, i.e., how many shrids are in the time series

/* sort in descending order of iso_sc_pc01 and find the top 10 shrids */
gsort -iso_sc_pc01
list shrid2 iso_sc_pc01 iso_sc_pc11 d_sc_pc01 d_sc_pc11 in 1/11, noobs abbrev(20)

gen byte flag = inlist(shrid2, ///
    "11-33-614-05774-803618", "11-33-624-05842-803762", ///
    "11-33-607-05732-803436", "11-14-276-01878-801484", ///
    "11-33-614-05775-803621") ///
  | inlist(shrid2, ///
    "11-33-624-05843-803766", "11-33-624-05843-803768", ///
    "11-33-605-05716-803393", "11-33-620-05813-803705", ///
    "11-19-336-02314-322318")



egen avg_d_pc01 = mean(d_sc_pc01)
egen avg_iso_pc01 = mean(iso_sc_pc01)

egen avg_d_pc11 = mean(d_sc_pc11)
egen avg_iso_pc11 = mean(iso_sc_pc11)


gen ln_sc_pop_shrid_pc01 = log(sc_pop_shrid_pc01)
gen ln_sc_pop_shrid_pc11 = log(sc_pop_shrid_pc11)

gen pct_sc_pop_shrid_pc01 = sc_pop_shrid_pc01 / tot_pop_shrid_pc01
gen pct_sc_pop_shrid_pc11 = sc_pop_shrid_pc11 / tot_pop_shrid_pc11


drop if iso_sc_pc01 > 2
drop if iso_sc_pc11 >2

drop if d_sc_pc01 <0.05
drop if d_sc_pc11 <0.05

twoway (scatter d_sc_pc01 pct_sc_pop_shrid_pc01, mcolor(navy%60) msize(small)), ///
       name(g1, replace) ///
       ytitle("Dissimilarity Index") ///
       xtitle("SC Population as % of Total Shrid Population (2001)") ///
       title("Dissimilarity Index (2001)")

twoway (scatter d_sc_pc11 pct_sc_pop_shrid_pc11, mcolor(maroon%60) msize(small)), ///
       name(g2, replace) ///
       ytitle("Dissimilarity Index") ///
       xtitle("SC Population as % of Total Shrid Population (2011)") ///
       title("Dissimilarity Index (2011)")

graph combine g1 g2, cols(1) ///
       title("Scheduled Castes Segregation by Population Share") ///
       ysize(8) xsize(6)
graphout d_scatter_combined, pdf



twoway (scatter iso_sc_pc01 pct_sc_pop_shrid_pc01, mcolor(navy%60) msize(small)), ///
       name(g1, replace) ///
       ytitle("Isolation Index") ///
       xtitle("SC Population as % of Total Shrid Population (2001)") ///
       title("Isolation Index (2001)")

twoway (scatter iso_sc_pc11 pct_sc_pop_shrid_pc11, mcolor(maroon%60) msize(small)), ///
       name(g2, replace) ///
       ytitle("Isolation Index") ///
       xtitle("SC Population as % of Total Shrid Population (2011)") ///
       title("Isolation Index (2011)")

graph combine g1 g2, cols(1) ///
       title("Scheduled Castes Segregation by Population Share") ///
       ysize(8) xsize(6)
graphout iso_scatter_combined, pdf

/* make histogram of segregation change */
gen change_dis = d_sc_pc11 - d_sc_pc01
gen change_iso = iso_sc_pc11 - iso_sc_pc01

list if abs(change_dis > 0.4)
list if abs(change_iso > 0.4)

/* Histograms (save each graph by name) */
histogram change_dis, ///
    percent bin(30) normal ///
    xtitle("Change in Dissimilarity (2001-2011)") ///
    ytitle("Percent") ///
    name(g_dis, replace)

histogram change_iso, ///
    percent bin(30) normal ///
    xtitle("Change in Isolation (2001-2011)") ///
    ytitle("Percent") ///
    name(g_iso, replace)

/* Combine side-by-side */
graph combine g_dis g_iso, ///
    col(2) iscale(1) imargin(2 2 2 2) ///
    title("Distribution of Segregation Changes")
graphout seg_change_combined, pdf



/* segregation change versus city size */
twoway (scatter change_dis pct_sc_pop_shrid_pc01,  mcolor(gs8%60) msymbol(o) msize(small) mlcolor(none)) ///
    (lfit change_dis pct_sc_pop_shrid_pc01, lcolor(gs4)), ///
       name(g1, replace) ///
       ytitle("Dissimilarity Index (2001-2011)") ///
       xtitle("SC Population as % of Total Shrid Population (2001)") ///
       title("Change in Dissimilarity Index (2001-2011)")

twoway (scatter change_iso pct_sc_pop_shrid_pc11,  mcolor(gs8%60) msymbol(o) msize(small) mlcolor(none)) ///
    (lfit change_iso pct_sc_pop_shrid_pc11, lcolor(gs4)), ///
       name(g2, replace) ///
       ytitle("Isolation Index (2001-2011)") ///
       xtitle("SC Population as % of Total Shrid Population (2011)") ///
       title("Change in Isolation Index (2001-2011)")

graph combine g1 g2, cols(1) ///
    title("Scheduled Castes Segregation Changes by Population Share") ///
    ysize(8) xsize(6)

graphout change_combined, pdf


/*
twoway (scatter d_sc_pc01 sc_pop_shrid_pc01, mcolor(navy%60) msize(small)) ///
       (scatter iso_sc_pc01 sc_pop_shrid_pc01, mcolor(maroon%60) msize(small) yaxis(2)), ///
       ytitle("Dissimilarity Index", axis(1)) ///
       ytitle("Isolation Index", axis(2)) ///
       xtitle("SC Population (shrid)") ///
       ylabel(, axis(1)) ///
       ylabel(0(0.2)2, axis(2)) ///
       legend(label(1 "Dissimilarity") label(2 "Isolation")) ///
       title("SC Segregation Indices by SC Population (PC01)")
graphout scatter_combined_pc01, pdf

/* Scatter plot: PC01 SC population vs dissimilarity */
scatter d_sc_pc01 pct_sc_pop_shrid_pc01, ///
    title("SC Segregation by SC Population (PC01)") ///
    xtitle("Percentage SC Population (shrid)") ///
    ytitle("Dissimilarity Index") ///
    msize(small) mcolor(navy%60)
graphout scatter_pct_d_sc_pc01, pdf

/* Scatter plot: PC11 SC population vs dissimilarity */
scatter d_sc_pc11 pct_sc_pop_shrid_pc11, ///
    title("SC Segregation by SC Population (PC11)") ///
    xtitle("Percentage SC Population (shrid)") ///
    ytitle("Dissimilarity Index") ///
    msize(small) mcolor(maroon%60)
graphout scatter_pct_d_sc_pc11, pdf

scatter iso_sc_pc01 ln_sc_pop_shrid_pc01, ///
    title("SC Segregation by SC Population (PC01)") ///
    xtitle("Log SC Population (shrid)") ///
    ytitle("Isolation Index") ///
    ylab(0(0.2)2) ///
    msize(small) mcolor(navy%60)
graphout scatter_iso_sc_pc01, pdf

/* Scatter plot: PC11 SC population vs dissimilarity */
scatter iso_sc_pc11 ln_sc_pop_shrid_pc11, ///
    title("SC Segregation by SC Population (PC11)") ///
    xtitle("Log SC Population (shrid)") ///
    ytitle("Isolation Index") ///
    ylab(0(0.2)2) ///
    msize(small) mcolor(maroon%60)
graphout scatter_iso_sc_pc11, pdf
*/

/* make a scatter plot that has sc_pop_shrid_pc01 on the x axis and d_sc_pc01 on the y axis */

/* make a scatter plot that has sc_pop_shrid_pc11 on the x axis and d_sc_pc11 on the y axis */

/* sort descending highest segregation first d_sc_pc01 */
/* sort descending highest segregation first iso_sc_pc01 */
/* and find the top ten highest shrid and the town names associated with them */

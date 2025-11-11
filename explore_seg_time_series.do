use $pc01/district_handbooks/pc01_11_merged_shrid_seg_cln, clear

/* PCA population vs. Handbook population */
merge 1:1 shrid using $frozen/shrug/v2.1.pakora/pca01/shrid/pc01_pca_clean_shrid, keepusing(pc01_pca_tot_p) keep(match master) nogen

/* get 2011 population */
merge 1:1 shrid using $frozen/shrug/v2.1.pakora/pca11/shrid/pc11_pca_clean_shrid, keepusing(pc11_pca_tot_p) keep(match master) nogen

/* Generate handbook population census error rate */
gen	 hb_error_01 = tot_pop_shrid_pc01 / pc01_pca_tot_p
gen	 hb_error_11 = tot_pop_shrid_pc11 / pc11_pca_tot_p

/* consider logging it if it is super skewed (e.g. 0.0001 -> 10000) */
gen ln_hb_error_01 = ln(hb_error_01)
gen ln_hb_error_11 = ln(hb_error_11)

/* drop places where we have very bad coverage */
keep if inrange(hb_error_01, .5, 1.5) &  inrange(hb_error_11, .5, 1.5) 

/* gen changes in segregation */
gen change_dis = d_sc_pc11 - d_sc_pc01
gen change_iso = iso_sc_pc11 - iso_sc_pc01

gen abs_change_dis = abs(change_dis)

/* scatter change in dissimilarity against handbook error */
scatter abs_change_dis ln_hb_error_01
graphout error_01

scatter abs_change_dis ln_hb_error_11
graphout error_11


sum change_dis, d

sum change_dis if inrange(hb_error_01, .5, 1.5) & inrange(hb_error_11, .5, 1.5), d

sum change_dis if inrange(hb_error_01, .8, 1.2) & inrange(hb_error_11, .8, 1.2), d


gen ln_pop_growth = ln(pc11_pca_tot_p) - ln(pc01_pca_tot_p)
gen ln_pop_01 = ln(pc01_pca_tot_p)

sum change_dis if inrange(ln_pop_growth, -.1, .1), d


binscatter change_dis ln_pop_growth
graphout dissim_growth

binscatter abs_change_dis ln_pop_growth, linetype(qfit)
graphout abs_dissim_growth

binscatter change_dis pct_sc_pop_shrid_pc01
graphout dis_sc_share


reg change_dis pct_sc_pop_shrid_pc01 ln_pop_01 ln_pop_growth

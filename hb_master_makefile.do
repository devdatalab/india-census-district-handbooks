* Master script to generate the 2001 urban EB dataset
version 14
clear
set more off


/***************************/
/* Run make for 2001 hb data */
/***************************/

/* 0. Create a clean version of the filename <> district name correspondence */
do $scode/b/handbooks/clean_filename_district_key.do

/* 1. Report initial coverage for hb of urban pca */
display "Running: catalog_hb.do" 
do $scode/b/handbooks/catalog_hb.do

/* 2. Find relevant eb pages in district handbooks */
display "Running: pc01_find_eb_pages.py"
shell python $scode/b/handbooks/pc01_find_eb_pages.py

/* 3. Clean relevant eb pages */
/* Sept 2025: the LLM seems to do fine without this step, so we're skipping it for now */
// display "Running: pc01_clean_hb_info.do"
// do $scode/b/handbooks/pc01_clean_hb_info.do

/* 4. Extract relevant eb pages */
display "extract_handbook_pages.py"
shell python $scode/b/handbooks/extract_handbook_pages.py

/* 5. LLM Extraction */
// not running now since some path updates needed and confirmation we're not re-parsing what we've done
// shell python $scode/b/handbooks/pc01_llm_extract/extract_pc01_tables.py

/* 6. Combine extracted csv */
display "Running: pc01_hb_combine.py"
do $scode/b/handbooks/pc01_hb_combine.py

/* 7. Merge hb town names to pca town directory */
display "Running: merge_handbook_towns.do"
do $scode/b/handbooks/merge_handbook_towns.do

/* 8. merge eb-lvl hb with $shrug/keys/shrug_pc01u_key to get shrid id, merge with secc 2011 to get segregation measures */
display "Running: calc_2001_segregation.do"
do $scode/b/calc_2001_segregation.do

/* 9. Create table analysis */
display "Running: analyze_seg_changes.do"
do $scode/a/analyze_seg_changes.do

/* This is the master run script for the handbook processing pipeline. */
/* Before running you must:
   (1) Load environment.yml
      - First time only:   conda env create -f environment.yml
      - Then each session: conda activate <env-name>
   (2) Export your API key in the shell used to launch Stata:
      - export GEMINI_API_KEY="sk-...your key..."
      - Then start Stata from that same shell
*/

/****************************************/
/* Run make file of hb for a given year */
/****************************************/

/* 0. Define series to the year we are building and load config */
do "~/india-census-district-handbooks/config.do"
hb_define_paths, series("pc01") // print out relevant paths

/* [Optional] Move the handbook pdfs we know that are (1) broken (2) have no table into archive subfolder */
di "Running: archive_broken_hb.py"
python script $hb_code/archive_broken_hb.py, args(`"--series $hb_series --pdf_root $hb_pdf"')

/* 1. Find relevant eb pages in district handbooks */
/* NOTE: Need to activate correct conda environment and install modules as needed */
di "Running: find_eb_pages.py"
python script $hb_code/find_eb_pages.py, args(`"--series $hb_series --pdf_root $hb_pdf --reprocess 0"')
//optional arg: --pdf_source_dir

/* [Optional] Manually adjust summary csv for ranges not correctly identified */
import delimited using "$hb_code/data/page_range_corrections.csv", varnames(1) clear
save "$tmp/page_range_corrections.dta", replace

import delimited using "$hb_pdf/${hb_series}_page_ranges_for_review.csv", varnames(1) clear
append using $tmp/page_range_corrections.dta

/* manual fix range find_eb_pages got wrong */
replace start_page = 360 if filename == "DH_22_2001_KAN.pdf"
replace end_page = 361 if filename == "DH_22_2001_KAN.pdf"

export delimited "$hb_pdf/${hb_series}_page_ranges_for_review.csv", replace

/* 2. Save relevant eb pages */
di "Running: extract_handbook_pages.py"
python script $hb_code/extract_handbook_pages.py, args(`"--series $hb_series --pdf_root $hb_pdf --pdf_source_dir taha_2025_09_19"')
// optional arg: --pdf_source_dir taha_2025_09_19

/* 3. LLM Extraction */
di "Running: llm_csv_hb_extractor.py"
python script $hb_code/llm_csv_hb_extractor.py, args(`"--series $hb_series --pdf_root $hb_pdf"')

/* 4. clean xls formatted handbooks into eb_table_extracts*/
di "Running process_xls_hb.py"
python script $hb_code/process_xls_hb.py, args(`"--series $hb_series --pdf_root $hb_pdf --xls_source_directory taha_2025_09_19"')

/* 5. Create a clean version of the filename <> district name correspondence */
di "Running: clean_filename_district_key.do"
do $hb_code/clean_filename_district_keys.do

/* 6. Combine extracted csv */
di "Running: combine_eb_tables.py"
python script $hb_code/combine_eb_tables.py, args(`"--series $hb_series --hb_code $hb_code --pdf_root $hb_pdf"')

/* 7. Merge with premade key, report coverage of urban pca */
di "Running: hb_merge_pca_coverage.do"
do $hb_code/hb_merge_pca_coverage.do

/* 7. Report initial coverage for hb of urban pca throughout pipeline */
di "Running: catalog_hb_data_loss.do" 
do $hb_code/catalog_hb_data_loss.do

/* 8. Convert coverage report into markdown table */
di "Running: make_attrition_report.py"
local in_dir "/dartfs-hpc/scratch/xinyu"

python script $hb_code/make_attrition_report.py, args(`"--series $hb_series --in_dir `in_dir' --out_dir $hb_code"')

exit


/* DEPENDENT DOWNSTREAM PROCESSES BELOW
/* 8. Merge hb town names to pca town directory */
display "Running: merge_handbook_towns.do"
do $scode/b/handbooks/merge_handbook_towns.do

/* 9. merge eb-lvl hb with $shrug/keys/shrug_pc01u_key to get shrid id, merge with secc 2011 to get segregation measures */
display "Running: calc_2001_segregation.do"
do $scode/b/calc_2001_segregation.do

/* 10. Create table analysis */
display "Running: analyze_seg_changes.do"
do $scode/a/analyze_seg_changes.do
*/

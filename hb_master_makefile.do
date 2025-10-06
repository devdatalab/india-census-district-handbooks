* Master script to generate the 2001 urban EB dataset
version 14
clear
set more off

/****************************************/
/* Run make file of hb for a given year */
/****************************************/

/* 0. Define series to the year we are building and load config */
do "~/india-census-district-handbooks/config.do"
hb_define_paths, series("pc11") // print out relevant paths


/* 1. Find relevant eb pages in district handbooks */
/* NOTE: Need to activate correct conda environment and install modules as needed */
di "Running: find_eb_pages.py"
python script $hb_code/find_eb_pages.py, args(`"--series $hb_series --pdf_root $hb_pdf"')


/* 2. Save relevant eb pages */
di "Running: extract_handbook_pages.py"
python script $hb_code/extract_handbook_pages.py, args(`"--series $hb_series --pdf_root $hb_pdf"')


/* 3. LLM Extraction */
/*
NOTE: Before running this step you must:
   (1) Load environment.yml
      - First time only:   conda env create -f environment.yml
      - Then each session: conda activate <env-name>
   (2) Export your API key in the shell used to launch Stata:
      - export GEMINI_API_KEY="sk-...your key..."
      - Then start Stata from that same shell
*/
di "Running: llm_csv_hb_extractor.py"
python script $hb_code/llm_csv_hb_extractor.py, args(`"--series $hb_series --pdf_root $hb_pdf"')

/* 4. Create a clean version of the filename <> district name correspondence */
di "Running: clean_filename_district_key.do"
do $hb_code/clean_filename_district_keys.do // skip for pc11 for now as filename <> district key stil wip


/* 5. Combine extracted csv */
di "Running: combine_eb_tables.py"
python script $hb_code/combine_eb_tables.py, args(`"--series $hb_series --hb_code $hb_code --pdf_root $hb_pdf"')
// similarly the second half of this script (combinging with district key) wouldn't work

/* 6. Report initial coverage for hb of urban pca throughout pipeline */
di "Running: catalog_hb_data_loss.do" 
do $hb_code/catalog_hb_data_loss.do // skip for pc11


/* 7. Convert coverage report into markdown table */
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

# India District Handbook Extraction Pipeline

This repository automates the extraction of Enumeration Block–level data from India’s District Census Handbooks 

The pipeline performs the following steps:

1. Locate District Handbook PDF files within their respective folders.  
2. Identify and extract the pages containing Urban Enumeration Block data.  
3. Save trimmed PDFs that include only the relevant pages.  
4. Use Gemini 2.5 Flash to extract structured tables from these pages into CSV.  
5. Combine all resulting tables into a unified dataset.  
6. Merge the outputs with the Primary Census Abstract (PCA) town directory using a curated key that maps handbook filenames to PCA districts.  
7. Generate reports on (a) coverage of urban districts and (b) data loss across processing stages.

---

## Running the Pipeline

`hb_master_makefile.do` orchestrates the workflow and defines the order in which all Stata and Python scripts are executed.

### Prerequisites

1. **Load the Conda environment**

   First time only:
     ```bash
     conda env create -f environment.yml -n <env-name>
     ```
   Each new session:
     ```bash
     conda activate <env-name>
     ```

2. **Export the Gemini API key**

   The Python scripts rely on the Google GenAI SDK for LLM extraction.  
   The API key must be exported as an environment variable before launching Stata:

   ```bash
   export GEMINI_API_KEY="sk-...your-key..."

## Build and Analysis Files

| File | Description |
|------|-------------|
| **config** | |
| `config.do` | Defines global macros used by downstream scripts for file operations.<br>**Usage:** `do config.do hb_define_paths, series(pc11)` |
| **b** | |
| `b/find_eb_pages.py` | Scans all District Handbook PDFs to locate pages containing Urban EB tables. Outputs a CSV (`filename`, `page_number`). Searches for phrases such as “URBAN BLOCK WISE” and “APPENDIX TO DISTRICT PRIMARY,” plus column header hints such as “LOCATION CODE” or “NAME OF TOWN.”<br>**Usage:** pass arguments like `--series`, `--pdf_root`, `--reprocess`. Use `--reprocess 1` to force full re-run; otherwise skips processed files by default. |
| `b/extract_handbook_pages.py` | Reads the identified page numbers and extracts the longest consecutive EB-page range to create a focused PDF. Outputs stored in `eb_table_extracts/` with `_EB` appended to filenames. |
| `b/llm_csv_hb_extractor.py` | Uses Gemini 2.5 Flash to extract clean, concatenated CSVs from EB-page PDFs. Requires `prompt_template.txt` and `extract_log.csv` for formatting and checkpointing. Flip `recreate_flag` to 1 to force re-run; on exception it sets `error_flag` and `recreate_flag` automatically. |
| `b/clean_filename_district_key.do` | Generates a standardized mapping between handbook filenames and PCA districts, saved as a `.dta` key file. |
| `b/process_xls_hb.py` | Processes `.xls` files into CSV and saves them alongside LLM output in the extracted-pages subdirectory. |
| `b/combine_eb_tables.py` | Combines all LLM-extracted EB tables into a single dataframe and merges them with state/district crosswalks, producing the unified `_file_manifest.csv`. |
| `b/make_seg_panel.do` | Creates the segregation analysis dataset: (1) fuzzy merge with urban PCA towns; (2) merge with `shrid2`; (3) merge PC91, PC01, PC11 to build the time series. |
| **a** | |
| `a/explore_seg_time_series.do` | Data quality checks and exploratory analysis of the segregation time series. |


## Data Coverage Reporting

`make_attrition_report.py` converts the output of  `loss_reporting/catalog_hb_data_loss.do` (`{series}_handbook_processing_loss.dta`)  
into a Markdown report (`{series}_hb_processing_report.md`) that summarizes: data loss across each pipeline stage, handbook coverage by district and state, and downstream merge rates for each pc series.
 
  [loss_reporting/reports/pc01_hb_processing_report.md](https://github.com/devdatalab/india-census-district-handbooks/blob/c130dfad741ab2656f049641648275dca5feed75/loss_reporting/reports/pc01_hb_processing_report.md)
 
  [loss_reporting/reports/pc11_hb_processing_report.md](https://github.com/devdatalab/india-census-district-handbooks/blob/c130dfad741ab2656f049641648275dca5feed75/loss_reporting/reports/pc11_hb_processing_report.md)


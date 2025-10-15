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

### Build Files

| File | Description |
|------|--------------|
| `config.do` | Defines global macros used by downstream scripts for file operations.<br>**Usage:** `do config.do hb_define_paths, series(pc11)` |
| `find_eb_pages.py` | Scans all District Handbook PDFs to locate pages containing Urban EB tables. Outputs a CSV with two columns (`filename`, `page_number`). Searches for phrases such as “URBAN BLOCK WISE” and “APPENDIX TO DISTRICT PRIMARY,” combined with column header hints such as “LOCATION CODE” or “NAME OF TOWN.”<br>**Usage:** `args("--series --pdf_root --reprocess")`. Use --reprocess 1 to force full re-run; otherwise skips processed files by default.|
| `extract_handbook_pages.py` | Reads the identified page numbers and extracts the longest consecutive range of EB pages to create a focused PDF. Outputs are saved in `eb_table_extracts/` within the input directory, with `_EB` appended to filenames. |
| `llm_csv_hb_extractor.py` | Uses Gemini 2.5 Flash to extract clean, concatenated CSVs from EB-page PDFs. Requires `prompt_template.txt` for prompt formatting and `extract_log.csv` for progress tracking and checkpointing: delete rows in extract_log.csv (or set a reprocess flag) to force the LLM to re-run for a file. |
| `clean_filename_district_key.do` | Generates a standardized mapping between handbook filenames and corresponding PCA districts, saved as a `.dta` key file. |
| `combine_eb_tables.py` | Combines all LLM-extracted EB tables into a single dataframe  and merges them with state and district crosswalks, producing a unified `_file_manifest.csv`. |
| `catalog_hb_data_loss.do` | Performs multi-stage data attrition tracking, assessing coverage and quality at each step from PDF presence to usable EB rows relative to the PCA urban district list. |
| `make_attrition_report.py` | Converts the output of `catalog_hb_data_loss.do` (`{series}_handbook_processing_loss.dta`) into a Markdown report (`{series}_hb_processing_report.md`) summarizing data loss and coverage across all stages. |



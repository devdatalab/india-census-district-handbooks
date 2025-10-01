/* this do file sets up config for district handbooks pipelines */
/* usage: do config.do
hb_define_paths, series(pc01) */


global hb_code "~/india-census-district-handbooks"

/* Default series if none supplied (pc01, pc11, pc51, pc91) */
global hb_series pc01
global iec "/dartfs/rc/lab/I/IEC"

/* Set all series-dependent paths */
capture program drop hb_define_paths
program define hb_define_paths
  syntax, [SERIES(name)] // if not specified, default is pc01

  /* Normalize series to lowercase */
  if "`series'" != "" {
    global hb_series = lower("`series'")
  }

  /* Validate series choice */
  if !inlist("$hb_series", "pc01", "pc11", "pc91") {  // add pc51 as they become ready
    di as err "Unknown series: $hb_series. Use one of: pc01 pc11 pc91."
    exit
  }

  if "$hb_series" == "pc01" {
    global hb_pdf $iec/pc01/district_handbooks
  }

  else if "$hb_series" == "pc11" {
    global hb_pdf $iec/pc11/district_handbooks_xii_b
  }

  else if "$hb_series" == "pc91" {
    global hb_pdf $iec/pc91/district_handbooks
  }

  else if "$hb_series" == "pc51" {
    global hb_pdf $iec/pc51/district_handbooks
  } // this is a placeholder path, need to update

  global hb_extracts   $hb_pdf/eb_table_extracts

  global hb_eb_pages_csv $hb_pdf/urban_eb_pages.csv

  /* Print paths  */
  di as txt "{hline 60}"
  di as res "HB Config"
  di as txt "{hline 60}"

  di as res "series:           "  as txt "$hb_series"
  di as res "hb_code:          "  as txt "$hb_code"

  di as res "hb_pdf:           " ///
      as txt "$hb_pdf" ///
      _newline "                 (dir containing the raw handbook PDFs)"

  di as res "hb_extracts:      " ///
      as txt "$hb_extracts" ///
      _newline "                 (dir containing identified page ranges with EB tables and LLM extracted CSVs)"

  di as res "hb_eb_pages_csv:  " ///
      as txt "$hb_eb_pages_csv" ///
      _newline "                 (CSV containing identified page ranges for each handbook PDF)"

  di as txt "{hline 60}"
end

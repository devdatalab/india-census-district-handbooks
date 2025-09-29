
/* loop over years */
foreach pc in pc91 pc01 {

  /* import the raw district key */
  import delimited using $scode/b/handbooks/`pc'_hb_pdf_keys.csv, ///
      varnames(1) stringcols(_all) clear

  replace district_name = lower(district_name)
  replace filename = substr(filename, 1, length(filename)-4)

  recast str244 filename

  duplicates drop filename, force
  save $tmp/`pc'_hb_pdf_key.dta, replace
}

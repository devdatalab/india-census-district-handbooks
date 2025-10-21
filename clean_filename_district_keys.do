/* This do file cleans human-made handbook pdfs to district keys */


/* import the raw district key */
import delimited using $hb_code/data/${hb_series}_hb_pdf_keys.csv, varnames(1) stringcols(_all) clear

replace district_name = lower(district_name)
replace filename = substr(filename, 1, length(filename)-4)

recast str244 filename

duplicates drop filename, force
save $tmp/${hb_series}_hb_pdf_key.dta, replace

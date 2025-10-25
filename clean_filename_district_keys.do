/* This do file cleans human-made handbook pdfs to district keys */


/* import the raw district key */
import delimited using $hb_code/data/${hb_series}_hb_pdf_keys.csv, varnames(1) stringcols(_all) clear

replace district_name = lower(district_name)
replace filename = substr(filename, 1, length(filename)-4)

recast str244 filename

/* drop _EB suffix */
replace filename = regexr(filename, "_EB\.(pdf|xls)$", "")

duplicates drop filename, force

rename (state_*) (`series'_state_*)
rename (district_*) (`series'_district_*)

save $tmp/${hb_series}_hb_pdf_key.dta, replace

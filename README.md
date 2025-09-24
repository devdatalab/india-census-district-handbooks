# india-census-district-handbooks
Repository of District Handbooks from the Indian Census

# ultimate goal: 100% complete demographic EB list for all locations in 1991, 2001, 2011

# next steps

## Get the build pipeline running
- Copy the complete DH pipeline from segregation to here
- Modify each step one at a time to take the year as a parameter
  - Create config.py and config.do, which have dictionaries assigning paths to years, e.g. dh_path['2001'] = '~/iec/...'

## Summarize status in markdown files
- create status_2001.md, vibe-code python to transform the `catalog_hb.do` into markdown that displays all the content nicely in Github


## Complete the dataset for every year
- Finish running 2011
- Finish running Taha's new files from 2001
- Explain dropoff from `has_pdf` -> `has_eb_pages` in the catalog file. This is 20% of the data -- what happened, how to fix?
  - If the file is corrupt, or wrong (like it's not a district handbook), move it into a `broken/` subfolder
- Create a Google Sheet (ask Toby where to put it, if we have a pc folder or something) with one row per year-district, with a column explaining what you learned about what is going on with this district. Eventually this could be a binary variable between `has_pdf` and `has_eb_pages` (change `has_eb_pages` -> `found_eb_pages`, and then the new column is `has_eb_pages`.
- Hunt for handbooks that we don't have, from the catalog list.
  - Scan and OCR the missing handbooks from the NCAER library

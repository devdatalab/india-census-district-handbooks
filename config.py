# Minimal config for District Handbooks
import os
from pathlib import Path

# choose series
SERIES = (os.environ.get("HB_SERIES") or "pc01").lower()  # set HB_SERIES=pc11 to switch
if SERIES not in {"pc01", "pc11", "pc91", "pc51"}:
    raise ValueError(f"Unknown series '{SERIES}'. Use one of: pc01, pc11, pc91, pc51")

# roots
HB_CODE  = Path(os.environ.get("HB_CODE",  Path.home() / "india-census-district-handbooks"))
IEC_ROOT = Path(os.environ.get("IEC_ROOT", "/dartfs/rc/lab/I/IEC"))

# per-series PDF root
if SERIES == "pc01":
    HB_PDF = IEC_ROOT / "pc01" / "district_handbooks"
elif SERIES == "pc11":
    HB_PDF = IEC_ROOT / "pc11" / "district_handbooks_xii_b"
elif SERIES == "pc91":
    HB_PDF = IEC_ROOT / "pc91" / "district_handbooks"
else:  # pc51 need adjusting
    HB_PDF = IEC_ROOT / "pc51" / "district_handbooks"

# Common paths
HB_EXTRACTS        = HB_PDF / "eb_table_extracts"
HB_URBAN_PAGES_CSV = HB_PDF / "urban_eb_pages.csv"

# Print when run directly
if __name__ == "__main__":
    print(f"series:     {SERIES}")
    print(f"hb_code:    {HB_CODE}")
    print(f"iec_root:   {IEC_ROOT}")
    print(f"pdf_root:   {HB_PDF}")
    print(f"extracts:   {HB_EXTRACTS}")
    print(f"pages_csv:  {HB_URBAN_PAGES_CSV}")

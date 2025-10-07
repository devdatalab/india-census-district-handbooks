# PC11 Handbook Processing — Minimal Data Loss Report

_Input_: `/dartfs-hpc/scratch/xinyu/pc11_handbook_processing_loss.dta`  
_Output_: `/dartfs-hpc/rc/home/w/f0083xw/india-census-district-handbooks/pc11_hb_processing_report.md`

## Drop Between Stages

| Between Stages | Dropped |
|---|---:|
| total → PDF present | 104 |
| PDF present → EB pages found | 1 |
| EB pages found → CSV extracted | 10 |
| CSV extracted → Reliable EB rows | 5 |

## Missing PDFs

| pc11_state_id | pc11_state_name | pc11_district_id | pc11_district_name | filename |
|---|---|---|---|---|
| 01 | jammu kashmir | 001 | kupwara |  |
| 01 | jammu kashmir | 002 | badgam |  |
| 01 | jammu kashmir | 003 | leh ladakh |  |
| 01 | jammu kashmir | 004 | kargil |  |
| 01 | jammu kashmir | 005 | punch |  |
| 01 | jammu kashmir | 006 | rajouri |  |
| 01 | jammu kashmir | 007 | kathua |  |
| 01 | jammu kashmir | 008 | baramula |  |
| 01 | jammu kashmir | 009 | bandipore |  |
| 01 | jammu kashmir | 010 | srinagar |  |
| 01 | jammu kashmir | 011 | ganderbal |  |
| 01 | jammu kashmir | 012 | pulwama |  |
| 01 | jammu kashmir | 013 | shupiyan |  |
| 01 | jammu kashmir | 014 | anantnag |  |
| 01 | jammu kashmir | 015 | kulgam |  |
| 01 | jammu kashmir | 016 | doda |  |
| 01 | jammu kashmir | 017 | ramban |  |
| 01 | jammu kashmir | 018 | kishtwar |  |
| 01 | jammu kashmir | 019 | udhampur |  |
| 01 | jammu kashmir | 020 | reasi |  |
| 01 | jammu kashmir | 021 | jammu |  |
| 01 | jammu kashmir | 022 | samba |  |
| 02 | himachal pradesh | 023 | chamba |  |
| 02 | himachal pradesh | 024 | kangra |  |
| 02 | himachal pradesh | 026 | kullu |  |
| 02 | himachal pradesh | 027 | mandi |  |
| 02 | himachal pradesh | 028 | hamirpur |  |
| 02 | himachal pradesh | 029 | una |  |
| 02 | himachal pradesh | 030 | bilaspur |  |
| 02 | himachal pradesh | 031 | solan |  |
| 02 | himachal pradesh | 032 | sirmaur |  |
| 02 | himachal pradesh | 033 | shimla |  |
| 03 | punjab | 035 | gurdaspur |  |
| 03 | punjab | 036 | kapurthala |  |
| 03 | punjab | 037 | jalandhar |  |
| 03 | punjab | 038 | hoshiarpur |  |
| 03 | punjab | 039 | shahid bhagat singh nagar |  |
| 03 | punjab | 040 | fatehgarh sahib |  |
| 03 | punjab | 041 | ludhiana |  |
| 03 | punjab | 042 | moga |  |
| 03 | punjab | 043 | firozpur |  |
| 03 | punjab | 044 | muktsar |  |
| 03 | punjab | 045 | faridkot |  |
| 03 | punjab | 046 | bathinda |  |
| 03 | punjab | 047 | mansa |  |
| 03 | punjab | 048 | patiala |  |
| 03 | punjab | 049 | amritsar |  |
| 03 | punjab | 050 | tarn taran |  |
| 03 | punjab | 051 | rupnagar |  |
| 03 | punjab | 052 | sahibzada ajit singh nagar |  |
| 03 | punjab | 053 | sangrur |  |
| 03 | punjab | 054 | barnala |  |
| 04 | chandigarh | 055 | chandigarh |  |
| 05 | uttarakhand | 056 | uttarkashi |  |
| 05 | uttarakhand | 057 | chamoli |  |
| 05 | uttarakhand | 058 | rudraprayag |  |
| 05 | uttarakhand | 059 | tehri garhwal |  |
| 05 | uttarakhand | 060 | dehradun |  |
| 05 | uttarakhand | 061 | garhwal |  |
| 05 | uttarakhand | 062 | pithoragarh |  |
| 05 | uttarakhand | 063 | bageshwar |  |
| 05 | uttarakhand | 064 | almora |  |
| 05 | uttarakhand | 065 | champawat |  |
| 05 | uttarakhand | 066 | nainital |  |
| 05 | uttarakhand | 067 | udham singh nagar |  |
| 05 | uttarakhand | 068 | hardwar |  |
| 06 | haryana | 069 | panchkula |  |
| 06 | haryana | 070 | ambala |  |
| 06 | haryana | 071 | yamunanagar |  |
| 06 | haryana | 072 | kurukshetra |  |
| 06 | haryana | 073 | kaithal |  |
| 06 | haryana | 074 | karnal |  |
| 06 | haryana | 075 | panipat |  |
| 06 | haryana | 076 | sonipat |  |
| 06 | haryana | 077 | jind |  |
| 06 | haryana | 078 | fatehabad |  |
| 06 | haryana | 079 | sirsa |  |
| 06 | haryana | 080 | hisar |  |
| 06 | haryana | 081 | bhiwani |  |
| 06 | haryana | 082 | rohtak |  |
| 06 | haryana | 083 | jhajjar |  |
| 06 | haryana | 084 | mahendragarh |  |
| 06 | haryana | 085 | rewari |  |
| 06 | haryana | 086 | gurgaon |  |
| 06 | haryana | 087 | mewat |  |
| 06 | haryana | 088 | faridabad |  |
| 06 | haryana | 089 | palwal |  |
| 07 | nct of delhi | 090 | north west |  |
| 07 | nct of delhi | 091 | north |  |
| 07 | nct of delhi | 092 | north east |  |
| 07 | nct of delhi | 093 | east |  |
| 07 | nct of delhi | 094 | new delhi |  |
| 07 | nct of delhi | 095 | central |  |
| 07 | nct of delhi | 096 | west |  |
| 07 | nct of delhi | 097 | south west |  |
| 07 | nct of delhi | 098 | south |  |
| 08 | rajasthan | 099 | ganganagar |  |
| 10 | bihar | 204 | purba champaran |  |
| 11 | sikkim | 242 | west district |  |
| 11 | sikkim | 243 | south district |  |
| 11 | sikkim | 244 | east district |  |
| 20 | jharkhand | 346 | garhwa |  |
| 22 | chhattisgarh | 405 | janjgir champa |  |
| 35 | andaman nicobar islands | 639 | north middle andaman |  |

## No EB pages (given PDFs)

| pc11_state_id | pc11_state_name | pc11_district_id | pc11_district_name | filename |
|---|---|---|---|---|
| 17 | meghalaya | 295 | south garo hills | DH_2011_1703_PART_B_DCHB_SOUTH_GARO_HILLS |

## No CSV (given EB pages)

| pc11_state_id | pc11_state_name | pc11_district_id | pc11_district_name | filename |
|---|---|---|---|---|
| 09 | uttar pradesh | 132 | saharanpur | DH_2011_0901_PART_B_DCHB_SAHARANPUR |
| 09 | uttar pradesh | 183 | gonda | DH_2011_0952_PART_B_DCHB_GONDA |
| 22 | chhattisgarh | 403 | raigarh | DH_2011_2204_PART_B_DCHB_RAIGARH |
| 27 | maharashtra | 515 | aurangabad | DH_2011_2719_PART_B_DCHB_AURANGABAD |
| 27 | maharashtra | 518 | mumbai suburban | DH_2011_2722_PART_B_DCHB_MUMBAI_SUBURBAN |
| 27 | maharashtra | 521 | pune | DH_2011_2725_PART_B_DCHB_PUNE |
| 28 | andhra pradesh | 538 | mahbubnagar | DH_2011_2807_PART_B_DCHB_MAHBUBNAGAR |
| 28 | andhra pradesh | 554 | chittoor | DH_2011_2823_PART_B_DCHB_CHITTOOR |
| 29 | karnataka | 572 | bangalore | DH_2011_2918_PART_B_DCHB_BANGALORE |
| 32 | kerala | 601 | thiruvananthapuram | DH_2011_3214_PART_B_DCHB_THIRUVANANTHAPURAM |

## No reliable EB rows (given CSV)

| pc11_state_id | pc11_state_name | pc11_district_id | pc11_district_name | filename |
|---|---|---|---|---|
| 12 | arunachal pradesh | 256 | kurung kumey | DH_2011_1212_PART_B_DCHB_KURUNG_KUMEY |
| 12 | arunachal pradesh | 257 | dibang valley | DH_2011_1213_PART_B_DCHB_DIBANG_VALLEY |
| 12 | arunachal pradesh | 260 | anjaw | DH_2011_1216_PART_B_DCHB_ANJAW |
| 29 | karnataka | 561 | gadag | DH_2011_2907_PART_B_DCHB_GADAG |
| 33 | tamil nadu | 617 | cuddalore | DH_2011_3316_PART_B_DCHB_CUDDALORE |

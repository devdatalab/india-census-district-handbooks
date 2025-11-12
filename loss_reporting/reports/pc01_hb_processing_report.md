# PC01 Handbook Processing — Markdown Report

## Overall Attrition Funnel (PDF → EB pages → CSV → Reliable rows)

| Stage | Kept | % of Total | Dropped from Prev |
|---|---:|---:|---:|
| 1. PDF present | 479 | 82.16% | 104 |
| 2. EB pages found | 463 | 79.42% | 16 |
| 3. CSV extracted | 461 | 79.07% | 2 |
| 4. Reliable EB rows | 449 | 77.02% | 12 |

## Town-level Downstream Coverage

| Metric | Count | Note |
|---|---:|---|
| Towns fuzzy-matched to urban PCA | 1144 | distinct `pc01_town_id` in `pc01_combined_hb_w_pca_cln.dta` |
| Towns with SHRID joined | 1144 | intersection with `pc01u_shrid_key.dta` |
| Towns in PC01×PC11 panel | 1040 | SHRID present in both `pc01` and `pc11` valid sets |

## Missing PDFs

| pc01_state_id | pc01_state_name | pc01_district_id | pc01_district_name | filename |
|---|---|---|---|---|
| 01 | jammu kashmir | 01 | kupwara |  |
| 01 | jammu kashmir | 02 | baramula |  |
| 01 | jammu kashmir | 03 | srinagar |  |
| 01 | jammu kashmir | 04 | badgam |  |
| 01 | jammu kashmir | 05 | pulwama |  |
| 01 | jammu kashmir | 06 | anantnag |  |
| 01 | jammu kashmir | 07 | leh ladakh |  |
| 01 | jammu kashmir | 08 | kargil |  |
| 01 | jammu kashmir | 09 | doda |  |
| 01 | jammu kashmir | 10 | udhampur |  |
| 01 | jammu kashmir | 11 | punch |  |
| 01 | jammu kashmir | 12 | rajauri |  |
| 01 | jammu kashmir | 13 | jammu |  |
| 01 | jammu kashmir | 14 | kathua |  |
| 03 | punjab | 07 | rupnagar |  |
| 05 | uttarakhand | 01 | uttarkashi |  |
| 05 | uttarakhand | 08 | bageshwar |  |
| 07 | delhi | 01 | north west |  |
| 07 | delhi | 02 | north |  |
| 07 | delhi | 03 | north east |  |
| 07 | delhi | 04 | east |  |
| 07 | delhi | 05 | new delhi |  |
| 07 | delhi | 06 | central |  |
| 07 | delhi | 07 | west |  |
| 07 | delhi | 08 | south west |  |
| 07 | delhi | 09 | south |  |
| 08 | rajasthan | 29 | chittaurgarh |  |
| 09 | uttar pradesh | 01 | saharanpur |  |
| 09 | uttar pradesh | 20 | bareilly |  |
| 09 | uttar pradesh | 23 | kheri |  |
| 09 | uttar pradesh | 42 | fatehpur |  |
| 09 | uttar pradesh | 45 | allahabad |  |
| 09 | uttar pradesh | 46 | barabanki |  |
| 09 | uttar pradesh | 47 | faizabad |  |
| 09 | uttar pradesh | 48 | ambedkar nagar |  |
| 09 | uttar pradesh | 59 | kushinagar |  |
| 09 | uttar pradesh | 61 | azamgarh |  |
| 09 | uttar pradesh | 64 | jaunpur |  |
| 09 | uttar pradesh | 66 | chandauli |  |
| 09 | uttar pradesh | 69 | mirzapur |  |
| 10 | bihar | 35 | gaya |  |
| 11 | sikkim | 02 | west |  |
| 11 | sikkim | 03 | south |  |
| 11 | sikkim | 04 | east |  |
| 14 | manipur | 04 | bishnupur |  |
| 14 | manipur | 06 | imphal west |  |
| 14 | manipur | 07 | imphal east |  |
| 14 | manipur | 09 | chandel |  |
| 17 | meghalaya | 03 | south garo hills |  |
| 20 | jharkhand | 02 | palamu |  |
| 20 | jharkhand | 04 | hazaribag |  |
| 20 | jharkhand | 07 | deoghar |  |
| 20 | jharkhand | 11 | dumka |  |
| 20 | jharkhand | 12 | dhanbad |  |
| 20 | jharkhand | 14 | ranchi |  |
| 20 | jharkhand | 17 | pashchimi singhbhum |  |
| 21 | orissa | 01 | bargarh |  |
| 21 | orissa | 02 | jharsuguda |  |
| 21 | orissa | 03 | sambalpur |  |
| 21 | orissa | 04 | debagarh |  |
| 21 | orissa | 05 | sundargarh |  |
| 21 | orissa | 06 | kendujhar |  |
| 21 | orissa | 07 | mayurbhanj |  |
| 21 | orissa | 08 | baleshwar |  |
| 21 | orissa | 09 | bhadrak |  |
| 21 | orissa | 10 | kendrapara |  |
| 21 | orissa | 11 | jagatsinghapur |  |
| 21 | orissa | 12 | cuttack |  |
| 21 | orissa | 15 | anugul |  |
| 21 | orissa | 16 | nayagarh |  |
| 21 | orissa | 17 | khordha |  |
| 21 | orissa | 18 | puri |  |
| 21 | orissa | 19 | ganjam |  |
| 21 | orissa | 20 | gajapati |  |
| 21 | orissa | 21 | kandhamal |  |
| 21 | orissa | 22 | baudh |  |
| 21 | orissa | 23 | sonapur |  |
| 21 | orissa | 24 | balangir |  |
| 21 | orissa | 26 | kalahandi |  |
| 21 | orissa | 27 | rayagada |  |
| 21 | orissa | 28 | nabarangapur |  |
| 21 | orissa | 29 | koraput |  |
| 21 | orissa | 30 | malkangiri |  |
| 22 | chhattisgarh | 02 | surguja |  |
| 22 | chhattisgarh | 04 | raigarh |  |
| 24 | gujarat | 02 | banas kantha |  |
| 24 | gujarat | 08 | surendranagar |  |
| 25 | daman diu | 02 | daman |  |
| 27 | maharashtra | 08 | wardha |  |
| 29 | karnataka | 03 | bijapur |  |
| 29 | karnataka | 24 | dakshina kannada |  |
| 32 | kerala | 06 | palakkad |  |
| 32 | kerala | 07 | thrissur |  |
| 32 | kerala | 08 | ernakulam |  |
| 32 | kerala | 10 | kottayam |  |
| 32 | kerala | 11 | alappuzha |  |
| 32 | kerala | 13 | kollam |  |
| 32 | kerala | 14 | thiruvananthapuram |  |
| 33 | tamil nadu | 08 | salem |  |
| 33 | tamil nadu | 22 | pudukkottai |  |
| 33 | tamil nadu | 26 | virudhunagar |  |
| 34 | pondicherry | 01 | yanam |  |
| 34 | pondicherry | 03 | mahe |  |
| 34 | pondicherry | 04 | karaikal |  |

## No EB pages (given PDFs)

| pc01_state_id | pc01_state_name | pc01_district_id | pc01_district_name | filename |
|---|---|---|---|---|
| 02 | himachal pradesh | 09 | solan | solan |
| 03 | punjab | 01 | gurdaspur | gurdaspur |
| 05 | uttarakhand | 02 | chamoli | chamoli |
| 05 | uttarakhand | 05 | dehradun | dehradun |
| 05 | uttarakhand | 07 | pithoragarh | Pithoragarh |
| 05 | uttarakhand | 09 | almora | almora |
| 05 | uttarakhand | 10 | champawat | Champawat |
| 05 | uttarakhand | 11 | nainital | Nainital |
| 08 | rajasthan | 24 | bhilwara | DH_08_2001_BHI |
| 19 | west bengal | 11 | north twenty four parganas | DH_19_2001_NTFP |
| 32 | kerala | 01 | kasaragod | Kasaragod_2001 |
| 32 | kerala | 02 | kannur | Kannur_2001 |
| 32 | kerala | 03 | wayanad | Wayanad_2001 |
| 32 | kerala | 04 | kozhikode | Kozhikode_2001 |
| 32 | kerala | 09 | idukki | Idukki_2001 |
| 32 | kerala | 12 | pathanamthitta | Pathanamthitta_2001 |

## No CSV (given EB pages)

| pc01_state_id | pc01_state_name | pc01_district_id | pc01_district_name | filename |
|---|---|---|---|---|
| 06 | haryana | 05 | kaithal | kaithal |
| 27 | maharashtra | 24 | raigarh | DH_27_2001_RAI |

## No reliable EB rows (given CSV)

| pc01_state_id | pc01_state_name | pc01_district_id | pc01_district_name | filename |
|---|---|---|---|---|
| 12 | arunachal pradesh | 01 | tawang | DH_12_2001_TAW |
| 12 | arunachal pradesh | 02 | west kameng | DH_12_2001_WKAM |
| 12 | arunachal pradesh | 03 | east kameng | DH_12_2001_EKAM |
| 12 | arunachal pradesh | 08 | east siang | DH_12_2001_ESIA |
| 12 | arunachal pradesh | 10 | dibang valley | DH_12_2001_DIB |
| 12 | arunachal pradesh | 11 | lohit | DH_12_2001_LOH |
| 16 | tripura | 02 | south tripura | DH_16_2001_STRI |
| 16 | tripura | 03 | dhalai | DH_16_2001_DHA |
| 16 | tripura | 04 | north tripura | DH_16_2001_NTRI |
| 20 | jharkhand | 18 | purbi singhbhum | DH_20_2001_PUR |
| 22 | chhattisgarh | 14 | kanker | DH_22_2001_KAN |
| 23 | madhya pradesh | 41 | dindori | DH_23_2001_DIN |

# src/data/ihme_all_cause_mortality.py
# Prepare 2019 IHME county-level all-cause mortality by sex, race and age group
#
# Source: https://ghdx.healthdata.org/record/ihme-data/united-states-causes-death-life-expectancy-by-county-race-ethnicity-2000-2019

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from config import SFBA_NAMES, IHME_AC_FILE, IHME_AC_OUT

# =============================================================================
# A. Load and filter
# =============================================================================

acm = pd.read_csv(IHME_AC_FILE)

acm = acm[acm["age_name"] != "Age-standardized"][
    ["fips", "location_name", "age_name", "sex_name", "race_name",
     "cause_name", "year", "val", "lower", "upper"]
].copy()

# =============================================================================
# B. Recode and clean
# =============================================================================

acm["location_name"] = acm["location_name"].str.replace(
    r"\s*County \(California\)\s*", "", regex=True
)

race_recode = {
    "AIAN":   "American Indian / Alaskan Native",
    "API":    "Asian / Pacific Islander",
    "Latino": "Hispanic"
}
acm["race_name"]  = acm["race_name"].replace(race_recode)
acm["age_name"]   = acm["age_name"].replace({"All Ages": "All ages"})
acm["geolevl"]    = "county"
acm["cause_name"] = "All-cause mortality"
acm["source"]     = "IHME"
acm["mx_name"]    = "person-year at risk"
acm["q_flag"]     = 0
acm["year"]       = acm["year"].astype(str)
acm = acm[acm["fips"].notna()]
acm["fips"] = acm["fips"].astype(float).astype(int).astype(str).str.zfill(5)
acm = acm[acm["location_name"].isin(SFBA_NAMES)]

# =============================================================================
# C. Rename and select final columns
# =============================================================================

acm = acm.rename(columns={
    "fips":          "geoid",
    "location_name": "lctn_nm",
    "age_name":      "age_grp",
    "sex_name":      "sex_grp",
    "race_name":     "race_grp",
    "cause_name":    "otcm_nm",
    "val":           "mx",
    "lower":         "mx_lower",
    "upper":         "mx_upper"
})[["geoid", "geolevl", "lctn_nm", "age_grp", "sex_grp", "race_grp",
    "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]]

# =============================================================================
# D. QA and save
# =============================================================================

print(f"Rows: {len(acm)}")
print(f"Counties: {acm['lctn_nm'].nunique()} (expected 9)")
print(f"Age groups: {sorted(acm['age_grp'].unique())}")
print(f"Race groups: {sorted(acm['race_grp'].unique())}")
print(acm[["mx", "mx_lower", "mx_upper"]].describe())

acm.to_parquet(IHME_AC_OUT, index=False)
print(f"Saved {len(acm)} rows to {IHME_AC_OUT}")

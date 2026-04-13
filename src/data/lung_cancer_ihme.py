# src/data/lung_cancer_ihme.py
# src/data/lung_cancer_ihme.py
# Clean 2019 IHME county-level lung cancer mortality
# Stratified by sex, race/ethnicity and age group
#
# Source: https://ghdx.healthdata.org/record/ihme-data/us-lung-cancer-county-race-ethnicity-2000-2019

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from config import SFBA_NAMES, LUNG_BOTH_FILE, LUNG_MALE_FILE, LUNG_FEMALE_FILE, LUNG_OUT

# =============================================================================
# A. Load and combine male, female and both sexes files
# =============================================================================

cols = ["fips", "location_name", "age_name", "sex_name",
        "race_name", "cause_name", "year", "metric_name", "val", "upper", "lower"]

lcan = pd.concat([
    pd.read_csv(f)[lambda df: df["location_name"].str.contains("California")][cols]
    for f in [LUNG_BOTH_FILE, LUNG_FEMALE_FILE, LUNG_MALE_FILE]
], ignore_index=True)

print(f"Loaded {len(lcan)} rows.")

# =============================================================================
# B. Clean and recode
# =============================================================================

lcan["location_name"] = lcan["location_name"].str.replace(
    " County (California)", "", regex=False
)

lcan["fips"] = lcan.apply(
    lambda r: str(int(r["fips"])).zfill(2)
    if r["location_name"] == "California"
    else str(int(r["fips"])).zfill(5),
    axis=1
)

lcan["geolevl"] = lcan["location_name"].apply(
    lambda x: "state" if x == "California" else "county"
)

race_recode = {
    "AIAN":   "American Indian / Alaskan Native",
    "Asian":  "Asian / Pacific Islander",
    "Latino": "Hispanic"
}
lcan["race_name"]  = lcan["race_name"].replace(race_recode)
lcan["age_name"]   = lcan["age_name"].replace({"All Ages": "All ages"})
lcan["cause_name"] = "Lung cancer mortality"
lcan["source"]     = "IHME"
lcan["mx_name"]    = "person-year at risk"
lcan["q_flag"]     = 0
lcan["year"]       = lcan["year"].astype(str)

lcan = lcan[
    lcan["location_name"].isin(SFBA_NAMES + ["California"]) &
    (lcan["age_name"] != "Age-standardized")
]

# =============================================================================
# C. Rename and select final columns
# =============================================================================

lcan = lcan.rename(columns={
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

print(f"Locations retained: {lcan['lctn_nm'].nunique()} (expected 9 counties + state)")
print(f"Sex groups: {lcan['sex_grp'].unique()}")
print(f"Age groups: {sorted(lcan['age_grp'].unique())}")
print(f"Missing rates: {lcan['mx'].isna().sum()}")
print(lcan[["mx", "mx_lower", "mx_upper"]].describe())

lcan.to_parquet(LUNG_OUT, index=False)
print(f"Saved {len(lcan)} rows to {LUNG_OUT}")

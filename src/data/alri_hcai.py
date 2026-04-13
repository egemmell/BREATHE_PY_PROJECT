# src/data/alri_hcai.py
# src/data/alri_hcai.py
# Clean 2019 HCAi ALRI data for children 0-17 in the San Francisco Bay Area
#
# Source: HCAi Patient Discharge (PDD) and Emergency Department and
#         Ambulatory Surgery (EDAS) Datasets (custom request CS3044)
# Includes ER visits and hospitalizations with primary ICD-10 diagnosis
# of acute lower respiratory infection (J09-J18, J20-J22)

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import pygris
from config import CA_FIPS, SFBA_FIPS, CENSUS_YEAR, ALRI_FILE, SEER_OUT, ALRI_OUT

# =============================================================================
# A. Load and initial clean
# =============================================================================

alri = pd.read_csv(ALRI_FILE, encoding="utf-8").rename(columns={
    "County":          "lctn_nm",
    "Race":            "race_grp",
    "Sex":             "sex_grp",
    "Number of Cases": "mx"
})

alri = alri[["lctn_nm", "race_grp", "sex_grp", "mx"]].copy()
alri["mx"] = alri["mx"].replace("<11", "10")
alri["mx"] = pd.to_numeric(alri["mx"], errors="coerce")

# =============================================================================
# B. Aggregate counts by group
# =============================================================================

# Sum ER visits and hospital admissions by county, race, sex
alri = (alri.groupby(["lctn_nm", "race_grp", "sex_grp"], as_index=False)
            ["mx"].sum())

# Add Both sex totals
both_sex = (alri.groupby(["lctn_nm", "race_grp"], as_index=False)
                ["mx"].sum()
                .assign(sex_grp="Both"))
alri = pd.concat([alri, both_sex], ignore_index=True)

# Add Total race totals
# Note: "Other" included in Total but excluded from rate calculations
total_race = (alri.groupby(["lctn_nm", "sex_grp"], as_index=False)
                  ["mx"].sum()
                  .assign(race_grp="Total"))
alri = pd.concat([alri, total_race], ignore_index=True)

# =============================================================================
# C. Recode and add constant columns
# =============================================================================

alri["race_grp"] = alri["race_grp"].replace(
    {"Asian/Pacific Islander": "Asian / Pacific Islander"}
)
alri["geolevl"]  = "county"
alri["age_grp"]  = "0 to 17"
alri["otcm_nm"]  = "Acute lower respiratory infection (children)"
alri["year"]     = "2019"
alri["source"]   = "HCAi"
alri["mx_name"]  = "prevalence"
alri["mx_lower"] = None
alri["mx_upper"] = None
alri["q_flag"]   = 0

# =============================================================================
# D. Add county FIPS codes via pygris
# =============================================================================

ca_counties = pygris.counties(state=CA_FIPS, year=CENSUS_YEAR)
fips_lookup = (ca_counties[ca_counties["GEOID"].isin(SFBA_FIPS)][["GEOID", "NAME"]]
               .rename(columns={"GEOID": "geoid", "NAME": "lctn_nm"}))
fips_lookup["geoid"] = fips_lookup["geoid"].str[2:]   # strip state prefix → 3-digit

alri = alri.merge(fips_lookup, on="lctn_nm", how="left")
alri["geoid"] = "06" + alri["geoid"]

# =============================================================================
# E. Merge with population data to calculate incidence rate
# =============================================================================

# Note: "Other" race excluded from rate calculation (no corresponding census
# population). "American Indian / Alaskan Native" not present in ALRI data.
demo = pd.read_parquet(SEER_OUT)[["geoid", "sex_grp", "race_grp", "population"]]

alri = alri[alri["race_grp"] != "Other"]
demo = demo[demo["race_grp"] != "American Indian / Alaskan Native"]

alri = alri.merge(demo, on=["geoid", "sex_grp", "race_grp"], how="left")
alri["mx"] = alri["mx"] / alri["population"]

alri = alri[["lctn_nm", "geolevl", "geoid", "age_grp", "race_grp", "sex_grp",
             "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]]

# =============================================================================
# F. QA and save
# =============================================================================

print(f"Counties retained: {alri['geoid'].nunique()} (expected 9)")
print(f"Missing incidence rates: {alri['mx'].isna().sum()}")
print(alri[["mx"]].describe())

alri.to_parquet(ALRI_OUT, index=False)
print(f"Saved {len(alri)} rows to {ALRI_OUT}")

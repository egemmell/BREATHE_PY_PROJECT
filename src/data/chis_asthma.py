# src/data/chis_asthma.py
# Clean California Health Interview Survey (CHIS) 2021-2022 asthma prevalence
# Adult (18+) and child (0-17) current asthma prevalence by county
#
# Source: CHIS via CKAN API
# Resource ID: a440b99b-ccc6-473c-bea1-2baf36b05dbe
#
# q_flag: 0 = reliable, 1 = missing (unreliable), 2 = statistically unstable
# Child county estimates are often unstable or missing — state-level more reliable
# Adult estimates: keep q_flag == 0 only
# Child estimates: keep q_flag 0 and 2 (unstable retained for sensitivity analyses)

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import re
import requests
import pandas as pd
import pygris
from config import SFBA_FIPS, SFBA_NAMES, CA_FIPS, CENSUS_YEAR, CHIS_ADULT_OUT, CHIS_CHILD_OUT

# =============================================================================
# A. Load county FIPS lookup via pygris
# =============================================================================

ca_counties = pygris.counties(state=CA_FIPS, year=CENSUS_YEAR)
fips_lookup = (ca_counties[ca_counties["GEOID"].isin(SFBA_FIPS)][["GEOID", "NAME"]]
               .rename(columns={"GEOID": "geoid", "NAME": "lctn_nm"}))

# =============================================================================
# B. CKAN download function
# =============================================================================

def fetch_all_ckan(resource_id, query, limit=500):
    base_url    = "https://data.chhs.ca.gov/api/3/action/datastore_search"
    offset      = 0
    all_records = []

    while True:
        params = {"resource_id": resource_id, "q": query,
                  "limit": limit, "offset": offset}
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        records = response.json()["result"]["records"]
        if not records:
            break
        all_records.extend(records)
        offset += limit
        print(f"  fetched {offset} rows so far...")

    return pd.DataFrame(all_records)

# =============================================================================
# C. Download CHIS data
# =============================================================================

print("Downloading CHIS 2021-2022 asthma data...")
chis = fetch_all_ckan("a440b99b-ccc6-473c-bea1-2baf36b05dbe", "2021-2022")
print(f"Downloaded {len(chis)} rows.")

# =============================================================================
# D. Shared cleaning function
# =============================================================================

def clean_chis_asthma(data, age_recode, keep_qflags, otcm_suffix=""):

    sfba_plus_state = SFBA_NAMES + ["California"]

    df = data[data["COUNTY"].isin(sfba_plus_state)].copy()

    # Parse confidence interval
    ci = (df["95% CONFIDENCE INTERVAL"]
          .str.replace("\u2013", "-", regex=False))   # replace en-dash
    df["mx_lower"] = ci.str.extract(r"([0-9.]+)-[0-9.]+")[0].astype(float)
    df["mx_upper"] = ci.str.extract(r"[0-9.]+- *([0-9.]+)")[0].astype(float)
    df["mx"]       = pd.to_numeric(df["CURRENT PREVALENCE"], errors="coerce")

    # Recode age groups
    df["age_raw"] = (df["AGE GROUP"]
                     .str.replace("\u2013", "-", regex=False))
    df = df[df["age_raw"].isin(age_recode.keys())].copy()
    df["age_grp"] = df["age_raw"].map(age_recode)

    # Assign q_flag
    def assign_qflag(comment):
        if pd.isna(comment):
            return 0
        if comment == "Prevalence not available due to unreliable estimate":
            return 1
        if "statistically unstable" in str(comment):
            return 2
        return 0

    df["q_flag"] = df["COMMENT"].apply(assign_qflag)
    df = df[df["q_flag"].isin(keep_qflags)].copy()

    # Add metadata columns
    df["otcm_nm"]  = f"Current asthma prevalence{otcm_suffix}"
    df["race_grp"] = "Total"
    df["sex_grp"]  = "Both"
    df["source"]   = "CHIS"
    df["mx_name"]  = "prevalence"
    df["geolevl"]  = df["COUNTY"].apply(lambda x: "state" if x == "California" else "county")
    df["year"]     = df["YEARS"].astype(str)

    # Merge FIPS
    df = df.merge(fips_lookup, left_on="COUNTY", right_on="lctn_nm", how="left")
    df["geoid"] = df.apply(
        lambda r: "06" if r["COUNTY"] == "California" else f"06{r['geoid']}",
        axis=1
    )

    return df[["geoid", "geolevl", "lctn_nm", "age_grp", "sex_grp", "race_grp",
               "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]]

# =============================================================================
# E. Clean adult asthma (18+)
# =============================================================================

adult_age_recode = {
    "18+ years":   "18 plus",
    "18-64 years": "18 to 64",
    "65+ years":   "65 plus"
}

asth_adult = clean_chis_asthma(
    data        = chis,
    age_recode  = adult_age_recode,
    keep_qflags = [0],
    otcm_suffix = " (adults)"
)

# Convert percent to proportion
for col in ["mx", "mx_lower", "mx_upper"]:
    asth_adult[col] = asth_adult[col] / 100

print(f"Adult asthma rows: {len(asth_adult)}")
print(f"Counties: {asth_adult['lctn_nm'].nunique()} (expected 9 + state)")
print(asth_adult[["mx", "mx_lower", "mx_upper"]].describe())

asth_adult.to_parquet(CHIS_ADULT_OUT, index=False)
print(f"Saved to {CHIS_ADULT_OUT}")

# =============================================================================
# F. Clean child asthma (0-17)
# =============================================================================

child_age_recode = {
    "0-4 years":  "0 to 4",
    "5-17 years": "5 to 17",
    "0-17 years": "0 to 17"
}

asth_child = clean_chis_asthma(
    data        = chis,
    age_recode  = child_age_recode,
    keep_qflags = [0, 2],
    otcm_suffix = " (children)"
)

for col in ["mx", "mx_lower", "mx_upper"]:
    asth_child[col] = asth_child[col] / 100

print(f"Child asthma rows: {len(asth_child)}")
print(f"Counties: {asth_child['lctn_nm'].nunique()}")
print(asth_child[["mx", "mx_lower", "mx_upper"]].describe())

asth_child.to_parquet(CHIS_CHILD_OUT, index=False)
print(f"Saved to {CHIS_CHILD_OUT}")
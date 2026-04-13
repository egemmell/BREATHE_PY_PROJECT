# src/data/cdc_places_asthma.py
# Download CDC PLACES 2021 tract- and county-level asthma prevalence for California
#
# Source: CDC PLACES 2021 release (model year 2019)
# Target: Current asthma prevalence + 95% CI (crude prevalence)

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import requests
import pandas as pd
from config import SFBA_NAMES, SFBA_GEOIDS, PLACES_OUT

# =============================================================================
# A. Configuration
# =============================================================================

endpoints = {
    "tract":  "https://data.cdc.gov/resource/373s-ayzu.json",
    "county": "https://data.cdc.gov/resource/pqpp-u99h.json"
}

fields = {
    "geoid":      "locationid",
    "lctn_nm":    "locationname",
    "state":      "stateabbr",
    "measure":    "measureid",
    "prevalence": "data_value",
    "mx_name":    "data_value_type",
    "ci_low":     "low_confidence_limit",
    "ci_high":    "high_confidence_limit"
}

CA_STATE   = "CA"
MEASURE_ID = "CASTHMA"
MX_TYPE    = "Crude prevalence"
BATCH_SIZE = 50000

# =============================================================================
# B. Download function
# =============================================================================

def download_places_paginated(endpoint, state, measure_id, mx_type,
                               fields, batch_size=50000):
    select_fields = ",".join(dict.fromkeys(fields.values()))  # unique, preserving order
    where_clause  = (f"{fields['state']}='{state}' AND "
                     f"{fields['measure']}='{measure_id}' AND "
                     f"{fields['mx_name']}='{mx_type}'")

    all_data = []
    offset   = 0
    page     = 1

    while True:
        print(f"Downloading batch {page} (offset {offset})...")
        response = requests.get(endpoint, params={
            "$select": select_fields,
            "$where":  where_clause,
            "$limit":  batch_size,
            "$offset": offset
        })
        response.raise_for_status()
        batch = response.json()

        if not batch:
            print("No more rows — download complete.")
            break

        all_data.extend(batch)
        print(f"  Retrieved {len(batch)} rows.")

        if len(batch) < batch_size:
            break
        offset += batch_size
        page   += 1

    return pd.DataFrame(all_data)

# =============================================================================
# C. Shared cleaning function
# =============================================================================

def clean_places(raw, geolevl, fields):
    df = raw.rename(columns={
        fields["geoid"]:      "geoid",
        fields["lctn_nm"]:    "lctn_nm",
        fields["mx_name"]:    "mx_name",
        fields["prevalence"]: "mx",
        fields["ci_low"]:     "mx_lower",
        fields["ci_high"]:    "mx_upper"
    }).copy()

    for col in ["mx", "mx_lower", "mx_upper"]:
        df[col] = pd.to_numeric(df[col], errors="coerce") / 100   # percent → proportion

    df["otcm_nm"]  = "Current asthma prevalence (adults)"
    df["source"]   = "CDC Places"
    df["geolevl"]  = geolevl
    df["age_grp"]  = "18 plus"
    df["sex_grp"]  = "Both"
    df["race_grp"] = "Total"
    df["year"]     = "2019"
    df["mx_name"]  = "prevalence"
    df["q_flag"]   = 0

    return df[["geoid", "geolevl", "lctn_nm", "age_grp", "sex_grp", "race_grp",
               "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]]

# =============================================================================
# D. Download and clean — tract level
# =============================================================================

print("Downloading CDC PLACES 2021 asthma data — census tracts...")
raw_tract = download_places_paginated(endpoints["tract"], CA_STATE,
                                      MEASURE_ID, MX_TYPE, fields, BATCH_SIZE)
print(f"Downloaded {len(raw_tract)} rows.")

places_tract = clean_places(raw_tract, geolevl="tract", fields=fields)
places_tract = places_tract[places_tract["geoid"].isin(SFBA_GEOIDS)]

print(f"SFBA tracts retrieved: {places_tract['geoid'].nunique()} (expected ~1,580)")
missing = places_tract["mx"].isna().sum()
if missing > 0:
    print(f"WARNING: {missing} tracts have missing asthma prevalence.")
print(places_tract[["mx", "mx_lower", "mx_upper"]].describe())

# =============================================================================
# E. Download and clean — county level
# =============================================================================

print("Downloading CDC PLACES 2021 asthma data — counties...")
raw_county = download_places_paginated(endpoints["county"], CA_STATE,
                                       MEASURE_ID, MX_TYPE, fields, BATCH_SIZE)
print(f"Downloaded {len(raw_county)} rows.")

places_county = clean_places(raw_county, geolevl="county", fields=fields)
places_county = places_county[places_county["lctn_nm"].isin(SFBA_NAMES)]

print(f"SFBA counties retained: {places_county['geoid'].nunique()} (expected 9)")
missing = places_county["mx"].isna().sum()
if missing > 0:
    print(f"WARNING: {missing} counties have missing asthma prevalence.")
print(places_county[["mx", "mx_lower", "mx_upper"]].describe())

# =============================================================================
# F. Combine and save
# =============================================================================

combined = pd.concat([places_tract, places_county], ignore_index=True)
combined.to_parquet(PLACES_OUT, index=False)
print(f"Saved {len(combined)} rows to {PLACES_OUT}")

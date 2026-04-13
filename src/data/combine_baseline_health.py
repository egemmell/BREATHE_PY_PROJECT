# src/data/combine_baseline_health.py
# src/data/combine_baseline_health.py
# Combine all cleaned baseline health outcome datasets
# for input into the BREATHE health impact assessment pipeline

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from config import (IHME_AC_OUT, USALEEP_OUT, IHD_OUT, LUNG_OUT,
                    PLACES_OUT, CHIS_ADULT_OUT, CHIS_CHILD_OUT, ALRI_OUT,
                    COMBINED_OUT)

# =============================================================================
# A. Load all datasets
# =============================================================================

# enforce consistent dtypes on load
dtype_spec = {
    "geoid":    str,
    "geolevl":  str,
    "lctn_nm":  str,
    "age_grp":  str,
    "sex_grp":  str,
    "race_grp": str,
    "otcm_nm":  str,
    "year":     str,
    "source":   str,
    "mx_name":  str,
    "mx":       float,
    "mx_lower": float,
    "mx_upper": float,
    "q_flag":   float
}

ac1   = pd.read_parquet(IHME_AC_OUT)
ac2   = pd.read_parquet(USALEEP_OUT)
ihd   = pd.read_parquet(IHD_OUT)
lcan  = pd.read_parquet(LUNG_OUT)
asth0 = pd.read_parquet(PLACES_OUT)
asth1 = pd.read_parquet(CHIS_ADULT_OUT)
asth2 = pd.read_parquet(CHIS_CHILD_OUT)
alri  = pd.read_parquet(ALRI_OUT)

print("Loaded all datasets:")
for name, df in [("IHME all-cause", ac1), ("USALEEP", ac2), ("IHD CDC", ihd),
                 ("Lung cancer", lcan), ("CDC PLACES", asth0),
                 ("CHIS adult", asth1), ("CHIS child", asth2), ("ALRI", alri)]:
    print(f"  {name}: {len(df)} rows")

# =============================================================================
# B. Combine and clean
# =============================================================================

all_outcomes = pd.concat([ac1, ac2, ihd, lcan, asth0, asth1, asth2, alri],
                         ignore_index=True)

# enforce dtypes after concat
for col, dtype in dtype_spec.items():
    if col in all_outcomes.columns:
        all_outcomes[col] = all_outcomes[col].astype(dtype, errors="ignore")

# drop rows with missing rates
all_outcomes = all_outcomes[all_outcomes["mx"].notna()]

# standardize mx_name labels
mx_name_recode = {
    "person-year at risk":  "deaths/person-year",
    "person-years at risk": "deaths/person-year",
    "prevalence":           "prevalence"
}
all_outcomes["mx_name"] = all_outcomes["mx_name"].replace(mx_name_recode)

# substitute mx for missing CIs
all_outcomes["mx_lower"] = all_outcomes["mx_lower"].fillna(all_outcomes["mx"])
all_outcomes["mx_upper"] = all_outcomes["mx_upper"].fillna(all_outcomes["mx"])

# exclude age-standardized rows
all_outcomes = all_outcomes[all_outcomes["age_grp"] != "Age-standardized"]

# =============================================================================
# C. QA checks
# =============================================================================

print("\nRow counts by outcome, source, geography, year and age:")
print(all_outcomes.groupby(["otcm_nm", "source", "geolevl", "year", "age_grp"])
                  .size()
                  .reset_index(name="n")
                  .to_string())

print(f"\nTotal rows: {len(all_outcomes)}")
print(f"Missing mx: {all_outcomes['mx'].isna().sum()}")
print(f"Missing mx_lower: {all_outcomes['mx_lower'].isna().sum()}")
print(f"Missing mx_upper: {all_outcomes['mx_upper'].isna().sum()}")

# check prevalence ranges — should be 0-1 not 0-100
prev = all_outcomes[all_outcomes["mx_name"] == "prevalence"]["mx"]
print(f"\nPrevalence range: min={prev.min():.4f}, max={prev.max():.4f}, mean={prev.mean():.4f}")

# =============================================================================
# D. Save
# =============================================================================

all_outcomes.to_parquet(COMBINED_OUT, index=False)
print(f"\nSaved {len(all_outcomes)} rows to {COMBINED_OUT}")

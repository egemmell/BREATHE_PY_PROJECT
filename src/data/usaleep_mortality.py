# src/data/usaleep_mortality.py
# src/data/usaleep_mortality.py
# Clean USALEEP all-cause mortality data (BenMAP-ready)
#
# Row = state-county FIPS (5 digits), Column = tract FIPS (6 digits)
# Concatenated to generate full 11-digit tract GEOID
# 2024 data release: mortality rates from 2010-2015, updated to 2020 population
# with 2010 census tract geography

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from config import CA_FIPS, SFBA_FIPS_3, USALEEP_FILE, USALEEP_OUT

# =============================================================================
# A. Load and recode
# =============================================================================

ac = pd.read_csv(USALEEP_FILE)

# Pad and combine FIPS codes into full 11-digit tract GEOID
ac["Row"]    = ac["Row"].astype(str).str.zfill(5)
ac["Column"] = ac["Column"].astype(str).str.zfill(6)
ac["geoid"]  = ac["Row"] + ac["Column"]

# Age groups
def assign_age_grp(row):
    s, e = row["Start Age"], row["End Age"]
    if   s ==  0 and e ==  0: return "<1 year"
    elif s ==  1 and e ==  4: return "1 to 4"
    elif s ==  5 and e == 14: return "5 to 14"
    elif s == 15 and e == 24: return "15 to 24"
    elif s == 25 and e == 34: return "25 to 34"
    elif s == 35 and e == 44: return "35 to 44"
    elif s == 45 and e == 54: return "45 to 54"
    elif s == 55 and e == 64: return "55 to 64"
    elif s == 65 and e == 74: return "65 to 74"
    elif s == 75 and e == 84: return "75 to 84"
    elif s == 85 and e == 99: return "85 to 99"
    else: return None

ac["age_grp"]  = ac.apply(assign_age_grp, axis=1)
ac["sex_grp"]  = "Both"
ac["race_grp"] = "Total"
ac["geolevl"]  = "tract"
ac["lctn_nm"]  = ac["Row"] + ac["Column"]
ac["year"]     = "2020"
ac["otcm_nm"]  = "All-cause mortality"
ac["source"]   = "USALEEP"
ac["mx_name"]  = "person-years at risk"
ac["mx_lower"] = None
ac["mx_upper"] = None
ac["q_flag"]   = 0

# =============================================================================
# B. Filter to SFBA tracts
# =============================================================================

ac = ac[
    (ac["geoid"].str[0:2] == CA_FIPS) &
    (ac["geoid"].str[2:5].isin(SFBA_FIPS_3))
]

# =============================================================================
# C. Select final columns
# =============================================================================

ac = ac.rename(columns={"Value": "mx"})[
    ["geoid", "geolevl", "lctn_nm", "age_grp", "sex_grp", "race_grp",
     "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]
]

# =============================================================================
# D. QA and save
# =============================================================================

print(f"Rows: {len(ac)}")
print(f"Tracts: {ac['geoid'].nunique()}")
print(f"Age groups: {sorted(ac['age_grp'].dropna().unique())}")
missing_age = ac["age_grp"].isna().sum()
if missing_age > 0:
    print(f"WARNING: {missing_age} rows with unmatched age groups.")
print(ac[["mx"]].describe())

ac.to_parquet(USALEEP_OUT, index=False)
print(f"Saved {len(ac)} rows to {USALEEP_OUT}")

# src/data/ihd_mortality_cdc.py
# src/data/ihd_mortality_cdc.py
# Clean 2019 CDC Wonder IHD mortality data
# State and county level, stratified by age, sex and race/ethnicity
#
# Sources:
#   State by age:           http://wonder.cdc.gov/controller/saved/D76/D474F974
#   County by age/sex/race: http://wonder.cdc.gov/controller/saved/D76/D476F903
#
# q_flag:
#   0 = CDC estimate (reliable)
#   1 = suppressed death count (1-9), imputed as 5 — CANNOT BE REPORTED
#   2 = death count < 20, rate flagged as unreliable — use with caution

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from config import (IHD_FILE, IHD_AGE_FILE, IHD_AGE_SEX_FILE, IHD_ALL_AGES_FILE,
                    IHD_ST_AGE_FILE, IHD_ST_AGE_SEX_FILE, IHD_ST_RACE_FILE, IHD_OUT)

# =============================================================================
# A. Shared cleaning function
# =============================================================================

SUPPRESSED = {"Suppressed", "Not Applicable", "Unreliable"}

def clean_cdc_wonder(path, geolevl, keep_rows=None, keep_cols=None,
                     has_hispanic=False, fixed_sex=None, fixed_race=None,
                     fixed_age=None, keep_qflags=(0,)):

    raw = pd.read_csv(path, dtype=str)

    # limit rows
    if keep_rows is not None:
        raw = raw.iloc[keep_rows]
    else:
        raw = raw[raw.iloc[:, 0].notna()]

    # select and rename columns
    raw = raw.rename(columns={v: k for k, v in keep_cols.items()})
    raw = raw[[k for k in keep_cols.keys()
               if k in raw.columns or k == "Hispanic Origin"]]

    # recode Hispanic into race column
    if has_hispanic:
        raw["race_grp"] = raw.apply(
            lambda r: "Hispanic"
            if r.get("Hispanic Origin") == "Hispanic or Latino"
            else r["race_grp"], axis=1
        )
        raw = raw.drop(columns=["Hispanic Origin"], errors="ignore")

    # add fixed columns
    if fixed_age  is not None: raw["age_grp"]  = fixed_age
    if fixed_sex  is not None: raw["sex_grp"]  = fixed_sex
    if fixed_race is not None: raw["race_grp"] = fixed_race

    # drop non-applicable population rows
    raw = raw[~raw["Population"].isin(["Not Applicable", "Suppressed"])].copy()

    # clean CI columns
    for ci_col in ["Crude Rate Lower 95% Confidence Interval",
                   "Crude Rate Upper 95% Confidence Interval"]:
        if ci_col in raw.columns:
            raw[ci_col] = raw[ci_col].apply(
                lambda x: None if x in SUPPRESSED else x
            )

    # coerce numeric columns
    raw["Population"] = pd.to_numeric(raw["Population"], errors="coerce")
    raw = raw[raw["Population"] > 0].copy()   # exclude zero population rows (rate undefined)
    raw["Deaths"]     = pd.to_numeric(
        raw["Deaths"].replace("Suppressed", "5"), errors="coerce"
    )
    
    # q_flag
    crude = raw["Crude Rate"]
    def assign_qflag(row):
        if row["Deaths"] == 5 and row["Crude Rate"] in SUPPRESSED:
            return 1
        if row["Crude Rate"] == "Unreliable":
            return 2
        return 0
    raw["q_flag"] = raw.apply(assign_qflag, axis=1)

    # impute rates for suppressed/unreliable
    raw["Crude Rate"] = raw.apply(
        lambda r: str(r["Deaths"] / r["Population"] * 100000)
        if r["Crude Rate"] in SUPPRESSED
        else r["Crude Rate"], axis=1
    )
    raw["Crude Rate"] = pd.to_numeric(raw["Crude Rate"], errors="coerce")

    # impute CIs for suppressed counts
    ci_low  = "Crude Rate Lower 95% Confidence Interval"
    ci_high = "Crude Rate Upper 95% Confidence Interval"
    if ci_low in raw.columns:
        raw[ci_low] = raw.apply(
            lambda r: r["Population"] and 1 / r["Population"] * 100000
            if r["q_flag"] == 1 else pd.to_numeric(r[ci_low], errors="coerce"),
            axis=1
        )
    if ci_high in raw.columns:
        raw[ci_high] = raw.apply(
            lambda r: r["Population"] and 9 / r["Population"] * 100000
            if r["q_flag"] == 1 else pd.to_numeric(r[ci_high], errors="coerce"),
            axis=1
        )

    # convert per 100k → per person-year
    raw["mx"]       = raw["Crude Rate"] / 100000
    raw["mx_lower"] = raw[ci_low]  / 100000 if ci_low  in raw.columns else None
    raw["mx_upper"] = raw[ci_high] / 100000 if ci_high in raw.columns else None

    # constant columns
    raw["geolevl"] = geolevl
    raw["otcm_nm"] = "Ischemic heart disease mortality"
    raw["year"]    = "2019"
    raw["source"]  = "CDC Wonder"
    raw["mx_name"] = "person-years at risk"

    # filter q_flags
    raw = raw[raw["q_flag"].isin(keep_qflags)]

    # pad geoid
    width = 2 if geolevl == "state" else 5
    raw["geoid"] = raw["geoid"].astype(str).str.zfill(width)

    return raw[["geoid", "geolevl", "lctn_nm", "age_grp", "sex_grp", "race_grp",
                "otcm_nm", "year", "source", "mx_name", "mx", "mx_lower", "mx_upper", "q_flag"]]

# =============================================================================
# B. Column mapping helper
# =============================================================================

CI_LOW  = "Crude Rate Lower 95% Confidence Interval"
CI_HIGH = "Crude Rate Upper 95% Confidence Interval"

BASE_COLS = {
    "Deaths":     "Deaths",
    "Population": "Population",
    "Crude Rate": "Crude Rate",
    CI_LOW:       CI_LOW,
    CI_HIGH:      CI_HIGH
}

# =============================================================================
# C. Load and clean each dataset
# =============================================================================

# State — age only
st_a = clean_cdc_wonder(
    path       = IHD_ST_AGE_FILE,
    geolevl    = "state",
    keep_rows  = range(11),
    keep_cols  = {"geoid": "State Code", "lctn_nm": "State",
                  "age_grp": "Ten-Year Age Groups", **BASE_COLS},
    fixed_sex  = "Both", fixed_race = "Total"
)

# State — age + sex
st_s = clean_cdc_wonder(
    path       = IHD_ST_AGE_SEX_FILE,
    geolevl    = "state",
    keep_rows  = range(22),
    keep_cols  = {"geoid": "State Code", "lctn_nm": "State",
                  "age_grp": "Ten-Year Age Groups", "sex_grp": "Sex", **BASE_COLS},
    fixed_race = "Total"
)

# State — age + sex + race
st_ihd = clean_cdc_wonder(
    path         = IHD_ST_RACE_FILE,
    geolevl      = "state",
    keep_rows    = range(288),
    keep_cols    = {"geoid": "State Code", "lctn_nm": "State",
                    "age_grp": "Ten-Year Age Groups", "sex_grp": "Sex",
                    "race_grp": "Race", "Hispanic Origin": "Hispanic Origin",
                    **BASE_COLS},
    has_hispanic = True
)

# County — age + sex (no race)
ihdb = clean_cdc_wonder(
    path       = IHD_AGE_SEX_FILE,
    geolevl    = "county",
    keep_rows  = range(216),
    keep_cols  = {"geoid": "County Code", "lctn_nm": "County",
                  "age_grp": "Ten-Year Age Groups", "sex_grp": "Sex", **BASE_COLS},
    fixed_race = "Total"
)
ihdb["lctn_nm"] = ihdb["lctn_nm"].str.replace(" County, CA", "", regex=False)

# County — age only
ihdc = clean_cdc_wonder(
    path       = IHD_AGE_FILE,
    geolevl    = "county",
    keep_rows  = range(108),
    keep_cols  = {"geoid": "County Code", "lctn_nm": "County",
                  "age_grp": "Ten-Year Age Groups", **BASE_COLS},
    fixed_sex  = "Both", fixed_race = "Total"
)
ihdc["lctn_nm"] = ihdc["lctn_nm"].str.replace(" County, CA", "", regex=False)

# County — age + sex + race
ihda = clean_cdc_wonder(
    path         = IHD_FILE,
    geolevl      = "county",
    keep_rows    = range(2592),
    keep_cols    = {"geoid": "County Code", "lctn_nm": "County",
                    "age_grp": "Ten-Year Age Groups", "sex_grp": "Sex",
                    "race_grp": "Race", "Hispanic Origin": "Hispanic Origin",
                    **BASE_COLS},
    has_hispanic = True
)
ihda["lctn_nm"] = ihda["lctn_nm"].str.replace(" County, CA", "", regex=False)

# County — unstratified
ihd_total = clean_cdc_wonder(
    path       = IHD_ALL_AGES_FILE,
    geolevl    = "county",
    keep_rows  = range(9),
    keep_cols  = {"geoid": "County Code", "lctn_nm": "County", **BASE_COLS},
    fixed_age  = "All ages", fixed_sex = "Both", fixed_race = "Total"
)
ihd_total["lctn_nm"] = ihd_total["lctn_nm"].str.replace(" County, CA", "", regex=False)

# =============================================================================
# D. Combine and recode age groups
# =============================================================================

ihd = pd.concat([st_a, st_s, st_ihd, ihda, ihdb, ihdc, ihd_total], ignore_index=True)

age_recode = {
    "25-34 years": "25 to 34",
    "35-44 years": "35 to 44",
    "45-54 years": "45 to 54",
    "55-64 years": "55 to 64",
    "65-74 years": "65 to 74",
    "75-84 years": "75 to 84",
    "85+ years":   "85 plus"
}
ihd["age_grp"] = ihd["age_grp"].replace(age_recode)

# =============================================================================
# E. QA and save
# =============================================================================

print(f"Total rows: {len(ihd)}")
print(f"State rows: {(ihd['geolevl'] == 'state').sum()}")
print(f"County rows: {(ihd['geolevl'] == 'county').sum()}")
print(f"Missing rates: {ihd['mx'].isna().sum()}")
print(ihd[["mx"]].describe())

ihd.to_parquet(IHD_OUT, index=False)
print(f"Saved {len(ihd)} rows to {IHD_OUT}")
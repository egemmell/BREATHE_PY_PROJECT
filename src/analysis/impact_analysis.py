# src/analysis/impact_analysis.py
# src/analysis/impact_analysis.py
# =============================================================================
# General Health Impact Assessment (HIA) Workflow
#
# Log-linear model:
#   y_h,i,a = m_h,i,a x P_i,a x (1 - exp(-beta_h,a x delta_x))
#
#   y       = attributable cases (deaths or prevalent cases)
#   m       = baseline rate per person (mx)
#   P       = population count per cell (geoid x age x sex x race)
#   beta    = ln(RR) / concentration increment
#   delta_x = change in pollutant concentration
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import numpy as np
import pandas as pd
from config import (COMBINED_OUT, EXP_POP_OUT,
                    HIA_RESULTS_OUT, HIA_TOTALS_OUT, HIA_BY_AGE_OUT,
                    HIA_BY_GEO_OUT, HIA_SOURCE_OUT, HIA_STRATA_OUT)

# =============================================================================
# A. Load data
# =============================================================================

print("Loading data...")
all_outcomes = pd.read_parquet(COMBINED_OUT)
merged       = pd.read_parquet(EXP_POP_OUT)
print(f"  Outcomes: {len(all_outcomes)} rows")
print(f"  Population: {len(merged)} persons")

# =============================================================================
# B. Age lookup tables
#    Maps single-year ages (0-99) to age group labels used by each source.
#    NA = age not covered by this outcome (person excluded from that run).
# =============================================================================

ages = list(range(100))

def make_lookup(conditions):
    """Build age→age_grp lookup dict from list of (condition_fn, label) pairs."""
    result = {}
    for age in ages:
        matched = None
        for cond, label in conditions:
            if cond(age):
                matched = label
                break
        result[age] = matched
    return result

age_lookup_usaleep = make_lookup([
    (lambda a: a == 0,           "<1 year"),
    (lambda a: 1  <= a <= 4,     "1 to 4"),
    (lambda a: 5  <= a <= 14,    "5 to 14"),
    (lambda a: 15 <= a <= 24,    "15 to 24"),
    (lambda a: 25 <= a <= 34,    "25 to 34"),
    (lambda a: 35 <= a <= 44,    "35 to 44"),
    (lambda a: 45 <= a <= 54,    "45 to 54"),
    (lambda a: 55 <= a <= 64,    "55 to 64"),
    (lambda a: 65 <= a <= 74,    "65 to 74"),
    (lambda a: 75 <= a <= 84,    "75 to 84"),
    (lambda a: a >= 85,          "85 to 99"),
])

age_lookup_ihme = make_lookup([
    (lambda a: a == 0,           "<1 year"),
    (lambda a: 1  <= a <= 4,     "1 to 4"),
    (lambda a: 5  <= a <= 9,     "5 to 9"),
    (lambda a: 10 <= a <= 14,    "10 to 14"),
    (lambda a: 15 <= a <= 19,    "15 to 19"),
    (lambda a: 20 <= a <= 24,    "20 to 24"),
    (lambda a: 25 <= a <= 29,    "25 to 29"),
    (lambda a: 30 <= a <= 34,    "30 to 34"),
    (lambda a: 35 <= a <= 39,    "35 to 39"),
    (lambda a: 40 <= a <= 44,    "40 to 44"),
    (lambda a: 45 <= a <= 49,    "45 to 49"),
    (lambda a: 50 <= a <= 54,    "50 to 54"),
    (lambda a: 55 <= a <= 59,    "55 to 59"),
    (lambda a: 60 <= a <= 64,    "60 to 64"),
    (lambda a: 65 <= a <= 69,    "65 to 69"),
    (lambda a: 70 <= a <= 74,    "70 to 74"),
    (lambda a: 75 <= a <= 79,    "75 to 79"),
    (lambda a: 80 <= a <= 84,    "80 to 84"),
    (lambda a: a >= 85,          "85 plus"),
])

age_lookup_cdcwonder = make_lookup([
    (lambda a: 25 <= a <= 34,    "25 to 34"),
    (lambda a: 35 <= a <= 44,    "35 to 44"),
    (lambda a: 45 <= a <= 54,    "45 to 54"),
    (lambda a: 55 <= a <= 64,    "55 to 64"),
    (lambda a: 65 <= a <= 74,    "65 to 74"),
    (lambda a: 75 <= a <= 84,    "75 to 84"),
    (lambda a: a >= 85,          "85 plus"),
    # <25 not in IHD data → None (excluded)
])

age_lookup_places = make_lookup([
    (lambda a: a >= 18,          "18 plus"),
    # children excluded → None
])

age_lookup_chis_granular = make_lookup([
    (lambda a: 0  <= a <= 4,     "0 to 4"),
    (lambda a: 5  <= a <= 17,    "5 to 17"),
    (lambda a: 18 <= a <= 64,    "18 to 64"),
    (lambda a: a >= 65,          "65 plus"),
])

age_lookup_chis_aggregate = make_lookup([
    (lambda a: 0  <= a <= 17,    "0 to 17"),
    (lambda a: a >= 18,          "18 plus"),
])

age_lookup_hcai = make_lookup([
    (lambda a: 0  <= a <= 17,    "0 to 17"),
    # adults excluded → None
])

age_lookups = {
    "USALEEP":        age_lookup_usaleep,
    "IHME":           age_lookup_ihme,
    "CDC Wonder":     age_lookup_cdcwonder,
    "CDC Places":     age_lookup_places,
    "CHIS_granular":  age_lookup_chis_granular,
    "CHIS_aggregate": age_lookup_chis_aggregate,
    "HCAi":           age_lookup_hcai
}

# =============================================================================
# C. Risk Ratio (RR) table
# =============================================================================

rr_data = [
    # All-cause mortality
    ("NO2",       "All-cause mortality",                            1.04, 1.01, 1.06, 10),
    ("BC",        "All-cause mortality",                            1.02, 1.00, 1.04,  1),
    ("TotalPM25", "All-cause mortality",                            1.03, 1.01, 1.05,  5),
    # IHD mortality
    ("NO2",       "Ischemic heart disease mortality",               1.05, 1.03, 1.08, 10),
    ("BC",        "Ischemic heart disease mortality",               1.05, 0.99, 1.11,  1),
    ("TotalPM25", "Ischemic heart disease mortality",               1.07, 1.04, 1.10,  5),
    # Lung cancer mortality
    ("NO2",       "Lung cancer mortality",                          1.04, 1.01, 1.07, 10),
    ("BC",        "Lung cancer mortality",                          1.02, 0.88, 1.19,  1),
    ("TotalPM25", "Lung cancer mortality",                          1.06, 0.99, 1.13,  5),
    # Adult asthma (NO2 only)
    ("NO2",       "Current asthma prevalence (adults)",             1.09, 1.03, 1.16, 10),
    # Child asthma (NO2 + BC)
    ("NO2",       "Current asthma prevalence (children)",           1.05, 0.99, 1.12, 10),
    ("BC",        "Current asthma prevalence (children)",           1.11, 0.94, 1.31,  1),
    # ALRI children (NO2 + BC)
    ("NO2",       "Acute lower respiratory infection (children)",   1.09, 1.03, 1.16, 10),
    ("BC",        "Acute lower respiratory infection (children)",   1.30, 0.78, 2.18,  1),
]

rr_table = pd.DataFrame(rr_data,
    columns=["exposure", "otcm_nm", "rr_central", "rr_lower", "rr_upper", "increment"])

rr_table["beta_central"] = np.log(rr_table["rr_central"]) / rr_table["increment"]
rr_table["beta_lower"]   = np.log(rr_table["rr_lower"])   / rr_table["increment"]
rr_table["beta_upper"]   = np.log(rr_table["rr_upper"])   / rr_table["increment"]

# =============================================================================
# D. Run specifications
# =============================================================================

run_specs = pd.DataFrame([
    # ── Unstratified primary analyses ─────────────────────────────────────────
    ("runU01", "All-cause mortality",                          "IHME",       "county", "geoid", "IHME",           [],                       0, True,  "PRIMARY: All-cause mortality | IHME | county | All ages",              "All ages / Both / Total"),
    ("runU02", "Ischemic heart disease mortality",             "CDC Wonder", "county", "geoid", "CDC Wonder",     [],                       0, True,  "PRIMARY: IHD mortality | CDC Wonder | county | All ages",              "All ages / Both / Total"),
    ("runU03", "Lung cancer mortality",                        "IHME",       "county", "geoid", "IHME",           [],                       0, True,  "PRIMARY: Lung cancer mortality | IHME | county | All ages",             "All ages / Both / Total"),
    # ── Stratified sensitivity analyses ──────────────────────────────────────
    ("run01",  "All-cause mortality",                          "USALEEP",    "tract",  "geoid", "USALEEP",        [],                       0, False, "All-cause mortality | USALEEP | tract | age",                          "Tract-level; no sex/race strata"),
    ("run02",  "All-cause mortality",                          "IHME",       "county", "geoid", "IHME",           [],                       0, False, "All-cause mortality | IHME | county | age",                            ""),
    ("run03",  "All-cause mortality",                          "IHME",       "county", "geoid", "IHME",           ["race_grp"],             0, False, "All-cause mortality | IHME | county | age + race",                     ""),
    ("run04",  "Current asthma prevalence (adults)",           "CDC Places", "tract",  "geoid", "CDC Places",     [],                       0, False, "Current asthma prevalence (adults) | CDC Places | tract | age (18+)",  "Adults only; tract level"),
    ("run05",  "Current asthma prevalence (adults)",           "CDC Places", "county", "geoid", "CDC Places",     [],                       0, False, "Current asthma prevalence (adults) | CDC Places | county | age (18+)", "Adults only"),
    ("run06",  "Current asthma prevalence (adults)",           "CHIS",       "county", "geoid", "CHIS_granular",  [],                       0, False, "Current asthma prevalence (adults) | CHIS | county | age (granular)",  "Non-overlapping: 18-64, 65+"),
    ("run07",  "Current asthma prevalence (adults)",           "CHIS",       "county", "geoid", "CHIS_aggregate", [],                       0, False, "Current asthma prevalence (adults) | CHIS | county | age (18+)",       "Comparable to CDC Places run05"),
    ("run08",  "Current asthma prevalence (adults)",           "CHIS",       "state",  "geoid", "CHIS_granular",  [],                       0, False, "Current asthma prevalence (adults) | CHIS | state | age (granular)",   ""),
    ("run09",  "Current asthma prevalence (adults)",           "CHIS",       "state",  "geoid", "CHIS_aggregate", [],                       0, False, "Current asthma prevalence (adults) | CHIS | state | age (18+)",        ""),
    ("run10",  "Current asthma prevalence (children)",         "CHIS",       "county", "geoid", "CHIS_granular",  [],                       0, False, "Current asthma prevalence (children) | CHIS | county | age (granular)","Non-overlapping: 0-4, 5-17"),
    ("run11",  "Current asthma prevalence (children)",         "CHIS",       "county", "geoid", "CHIS_aggregate", [],                       0, False, "Current asthma prevalence (children) | CHIS | county | age (0-17)",    ""),
    ("run12",  "Current asthma prevalence (children)",         "CHIS",       "state",  "geoid", "CHIS_granular",  [],                       0, False, "Current asthma prevalence (children) | CHIS | state | age (granular)", ""),
    ("run13",  "Current asthma prevalence (children)",         "CHIS",       "state",  "geoid", "CHIS_aggregate", [],                       0, False, "Current asthma prevalence (children) | CHIS | state | age (0-17)",     ""),
    ("run14",  "Ischemic heart disease mortality",             "CDC Wonder", "county", "geoid", "CDC Wonder",     [],                       0, False, "IHD mortality | CDC Wonder | county | age",                            "Age groups 45+ only"),
    ("run15",  "Ischemic heart disease mortality",             "CDC Wonder", "county", "geoid", "CDC Wonder",     ["sex_grp"],              0, False, "IHD mortality | CDC Wonder | county | age + sex",                      "Age groups 45+ only"),
    ("run16",  "Ischemic heart disease mortality",             "CDC Wonder", "state",  "geoid", "CDC Wonder",     [],                       0, False, "IHD mortality | CDC Wonder | state | age",                             "Age groups 25+ at state level"),
    ("run17",  "Ischemic heart disease mortality",             "CDC Wonder", "state",  "geoid", "CDC Wonder",     ["sex_grp"],              0, False, "IHD mortality | CDC Wonder | state | age + sex",                       ""),
    ("run18",  "Lung cancer mortality",                        "IHME",       "county", "geoid", "IHME",           [],                       0, False, "Lung cancer mortality | IHME | county | age",                          ""),
    ("run19",  "Lung cancer mortality",                        "IHME",       "county", "geoid", "IHME",           ["sex_grp"],              0, False, "Lung cancer mortality | IHME | county | age + sex",                    ""),
    ("run20",  "Lung cancer mortality",                        "IHME",       "county", "geoid", "IHME",           ["race_grp"],             0, False, "Lung cancer mortality | IHME | county | age + race",                   ""),
    ("run21",  "Lung cancer mortality",                        "IHME",       "county", "geoid", "IHME",           ["sex_grp", "race_grp"],  0, False, "Lung cancer mortality | IHME | county | age + sex + race",             ""),
    ("run22",  "Acute lower respiratory infection (children)", "HCAi",       "county", "geoid", "HCAi",           [],                       0, False, "ALRI children | HCAi | county | age (0-17)",                           "Children only"),
    ("run23",  "Acute lower respiratory infection (children)", "HCAi",       "county", "geoid", "HCAi",           ["sex_grp"],              0, False, "ALRI children | HCAi | county | age + sex",                            "Children only"),
], columns=["run_id", "otcm_nm", "source", "geolevl", "geo_col",
            "age_lookup_key", "strata", "q_flag_max", "unstratified", "label", "note"])

# =============================================================================
# E. Core HIA function
# =============================================================================

def run_hia(merged_data, all_outcomes, run_row, rr_row, exposure_col):

    run_id       = run_row["run_id"]
    otcm_nm      = run_row["otcm_nm"]
    source       = run_row["source"]
    geolevl      = run_row["geolevl"]
    geo_col      = run_row["geo_col"]
    age_lkp_key  = run_row["age_lookup_key"]
    q_flag_max   = run_row["q_flag_max"]
    label        = run_row["label"]
    note         = run_row["note"]
    unstratified = run_row["unstratified"]
    strata       = run_row["strata"]

    # --- E1. Filter outcome data ---------------------------------------------
    outcome = all_outcomes[
        (all_outcomes["otcm_nm"] == otcm_nm) &
        (all_outcomes["source"]  == source)  &
        (all_outcomes["geolevl"] == geolevl) &
        (all_outcomes["q_flag"]  <= q_flag_max)
    ].copy()

    if unstratified:
        outcome = outcome[
            (outcome["age_grp"]  == "All ages") &
            (outcome["sex_grp"]  == "Both") &
            (outcome["race_grp"] == "Total")
        ]

    if len(outcome) == 0:
        print(f"  [{run_id}] No outcome data found — skipping.")
        return None

    # --- E2. Assign age groups / filter valid exposures ----------------------
    age_lkp = age_lookups[age_lkp_key]

    pop = merged_data[merged_data[exposure_col].notna()].copy()

    # truncate geoid to geography level
    if geolevl == "county":
        pop["geoid"] = pop["geoid"].str[:5]
    elif geolevl == "state":
        pop["geoid"] = pop["geoid"].str[:2]

    if unstratified:
        group_cols    = [geo_col]
        active_strata = []
        strata_used   = "none (All ages / Both / Total)"
    else:
        pop["age_grp"] = pop["age"].map(age_lkp)
        pop = pop[pop["age_grp"].notna()]

        # --- E3. Determine active strata -------------------------------------
        active_strata = [
            s for s in strata
            if s in outcome.columns and outcome[s].nunique() > 1
        ]
        group_cols  = [geo_col, "age_grp"] + active_strata
        strata_used = "age only" if not active_strata \
                      else " + ".join(["age"] + active_strata)

        # --- E4. Rename sex/race where needed --------------------------------
        if "sex_grp" in active_strata and "sex_grp" not in pop.columns:
            pop = pop.rename(columns={"sex": "sex_grp"})
        if "race_grp" in active_strata and "race_grp" not in pop.columns:
            pop = pop.rename(columns={"race": "race_grp"})

    if len(pop) == 0:
        print(f"  [{run_id}] No persons matched — skipping.")
        return None

    # --- E4. Aggregate persons to population cells ---------------------------
    merge_group_cols = [c for c in group_cols if c in pop.columns]

    pop_cells = (pop.groupby(merge_group_cols, as_index=False)
                    .agg(P=(exposure_col, "count"),
                         delta_x=(exposure_col, "mean")))

    # --- E5. Join baseline rates ---------------------------------------------
    outcome_slim = (outcome
                    .rename(columns={"geoid": geo_col})
                    [[geo_col, "age_grp"] + active_strata +
                     ["mx", "mx_lower", "mx_upper", "q_flag", "year"]]
                    .rename(columns={"year": "outcome_year"})
                    if not unstratified else
                    outcome
                    .rename(columns={"geoid": geo_col})
                    [[geo_col, "mx", "mx_lower", "mx_upper", "q_flag", "year"]]
                    .rename(columns={"year": "outcome_year"}))

    pop_cells = pop_cells.merge(outcome_slim, on=group_cols, how="left")

    # --- E6. Apply log-linear HIA formula ------------------------------------
    pop_cells = pop_cells[pop_cells["mx"].notna()].copy()

    if len(pop_cells) == 0:
        print(f"  [{run_id} | {exposure_col}] No rows after mx filter — skipping.")
        return None

    pop_cells["af_central"] = 1 - np.exp(-rr_row["beta_central"] * pop_cells["delta_x"])
    pop_cells["af_lower"]   = 1 - np.exp(-rr_row["beta_lower"]   * pop_cells["delta_x"])
    pop_cells["af_upper"]   = 1 - np.exp(-rr_row["beta_upper"]   * pop_cells["delta_x"])

    pop_cells["y_central"]  = pop_cells["mx"]       * pop_cells["P"] * pop_cells["af_central"]
    pop_cells["y_lower"]    = pop_cells["mx_lower"]  * pop_cells["P"] * pop_cells["af_lower"]
    pop_cells["y_upper"]    = pop_cells["mx_upper"]  * pop_cells["P"] * pop_cells["af_upper"]

    pop_cells["run_id"]     = run_id
    pop_cells["run_label"]  = label
    pop_cells["run_note"]   = note
    pop_cells["otcm_nm"]    = otcm_nm
    pop_cells["source"]     = source
    pop_cells["exposure"]   = exposure_col
    pop_cells["geo_col"]    = geo_col
    pop_cells["strata_used"]= strata_used
    pop_cells["geolevl"]    = geolevl
    pop_cells["analysis"]   = "primary" if unstratified else "sensitivity"

    return pop_cells

# =============================================================================
# F. Execute all runs
# =============================================================================

all_results = []

for _, run_row in run_specs.iterrows():
    run_id  = run_row["run_id"]
    otcm_nm = run_row["otcm_nm"]
    label   = run_row["label"]

    rr_rows = rr_table[rr_table["otcm_nm"] == otcm_nm]

    if len(rr_rows) == 0:
        print(f"[{run_id}] No RR found for {otcm_nm} — skipping.")
        continue

    print(f"\n=== {run_id}: {label} ===")

    for _, rr_row in rr_rows.iterrows():
        result = run_hia(
            merged_data  = merged,
            all_outcomes = all_outcomes,
            run_row      = run_row,
            rr_row       = rr_row,
            exposure_col = rr_row["exposure"]
        )
        if result is not None:
            all_results.append(result)

hia_results = pd.concat(all_results, ignore_index=True)
print(f"\nTotal runs completed: {hia_results['run_id'].nunique()}")
print(f"Total output rows:    {len(hia_results)}")

# =============================================================================
# G. Summary tables
# =============================================================================

# H1. Totals per run x outcome x exposure
hia_totals = (hia_results
    .groupby(["run_id", "run_label", "run_note", "otcm_nm", "source",
              "exposure", "geo_col", "strata_used", "outcome_year"], as_index=False)
    .agg(n_cells=("P", "count"),
         P_total=("P", "sum"),
         y_central=("y_central", "sum"),
         y_lower=("y_lower", "sum"),
         y_upper=("y_upper", "sum"))
    .sort_values(["otcm_nm", "exposure", "run_id"]))

print("\nTotals by run:")
print(hia_totals[["run_id", "run_label", "otcm_nm", "exposure",
                   "y_central", "y_lower", "y_upper", "P_total", "n_cells"]]
      .to_string())

# H2. By age group
hia_by_age = (hia_results[hia_results["analysis"] == "sensitivity"]
    .groupby(["run_id", "run_label", "otcm_nm", "source", "exposure",
              "geo_col", "strata_used", "age_grp"], as_index=False)
    .agg(P=("P", "sum"),
         y_central=("y_central", "sum"),
         y_lower=("y_lower", "sum"),
         y_upper=("y_upper", "sum")))

# H3. By geography cell
geo_col_val = hia_results["geo_col"].iloc[0]
hia_by_geo = (hia_results
    .groupby(["run_id", "run_label", "otcm_nm", "source", "exposure",
              "geo_col", "strata_used", geo_col_val], as_index=False)
    .agg(P=("P", "sum"),
         y_central=("y_central", "sum"),
         y_lower=("y_lower", "sum"),
         y_upper=("y_upper", "sum")))

# H4. Source sensitivity comparison
hia_source_comparison = (hia_totals[
    hia_totals["otcm_nm"].isin([
        "All-cause mortality",
        "Current asthma prevalence (adults)",
        "Current asthma prevalence (children)"
    ])]
    [["run_id", "run_label", "otcm_nm", "source", "exposure",
      "geo_col", "y_central", "y_lower", "y_upper"]]
    .sort_values(["otcm_nm", "exposure", "geo_col", "run_id"]))

print("\nSource sensitivity comparison:")
print(hia_source_comparison.to_string())

# H5. Stratification sensitivity comparison
hia_strata_comparison = (hia_totals
    [["run_id", "run_label", "otcm_nm", "source", "geo_col",
      "strata_used", "exposure", "y_central", "y_lower", "y_upper"]]
    .sort_values(["otcm_nm", "source", "geo_col", "strata_used"]))

print("\nStratification sensitivity comparison:")
print(hia_strata_comparison.to_string())

# =============================================================================
# H. Write outputs
# =============================================================================

hia_results.to_parquet(HIA_RESULTS_OUT, index=False)
hia_totals.to_parquet(HIA_TOTALS_OUT, index=False)
hia_by_age.to_parquet(HIA_BY_AGE_OUT, index=False)
hia_by_geo.to_parquet(HIA_BY_GEO_OUT, index=False)
hia_source_comparison.to_parquet(HIA_SOURCE_OUT, index=False)
hia_strata_comparison.to_parquet(HIA_STRATA_OUT, index=False)

print("\nHIA workflow complete.")
print(f"Total runs completed: {hia_results['run_id'].nunique()}")
print(f"Total output rows:    {len(hia_results)}")

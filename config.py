# config.py
# Central configuration file for BREATHE HIA pipeline
# Import this at the top of each script: from config import *


import pygris

# SFBA census tract GEOIDs (loaded once here for use across scripts)
_tracts = pygris.tracts(state=CA_FIPS, year=CENSUS_YEAR)
SFBA_GEOIDS = _tracts[_tracts["COUNTYFP"].isin(SFBA_FIPS_3)]["GEOID"].tolist()

# San Francisco Bay Area county FIPS codes (9 counties)
SFBA_FIPS = ["06001", "06013", "06041", "06055", "06075",
             "06081", "06085", "06095", "06097"]

# County-only FIPS (3-digit, for tract-level filtering)
SFBA_FIPS_3 = ["001", "013", "041", "055", "075",
               "081", "085", "095", "097"]

# County names (for datasets that use names instead of FIPS)
SFBA_NAMES = ["Alameda", "Contra Costa", "Marin", "Napa",
              "San Francisco", "San Mateo", "Santa Clara",
              "Solano", "Sonoma"]

# Named dict linking FIPS to names (useful for joins and labels)
SFBA_LOOKUP = dict(zip(SFBA_FIPS, SFBA_NAMES))

# State FIPS
CA_FIPS = "06"

# Census year
CENSUS_YEAR = 2019

# ── File paths ────────────────────────────────────────────────────────────────
# Raw data inputs
SEER_FILE       = "data/raw/population_data/ca.1990_2024.singleages.through89.90plus.txt"

# Processed outputs (scripts 1-9 write here)
SEER_OUT        = "data/processed/pop_0_17_sex_race_county_2019.parquet"
CHIS_ADULT_OUT  = "data/processed/adult_currentasthma_state_county_2021_2022_CHIS.parquet"
CHIS_CHILD_OUT  = "data/processed/child_currentasthma_state_county_2021_2022_CHIS.parquet"
PLACES_OUT = "data/processed/adult_currentasthma_ctract_county_2019_CDCPlaces.parquet"
IHME_AC_OUT     = "data/processed/all_cause_mortality_2019_ihme.parquet"
USALEEP_OUT     = "data/processed/all_cause_mortality_2020_usaleep.parquet"
ALRI_OUT        = "data/processed/alri_2019_hcai.parquet"
IHD_OUT         = "data/processed/ihd_mortality_2019_cdc.parquet"
LUNG_OUT        = "data/processed/lung_cancer_mortality_2019_ihme.parquet"
COMBINED_OUT    = "data/processed/baseline_health_combined.parquet"

# Analysis outputs
EXP_POP_OUT     = "data/processed/exposure_population_inputs.parquet"
IMPACT_OUT      = "output/impact_results.parquet"

# config.py
# Central configuration file for BREATHE HIA pipeline
# Import this at the top of each script: from config import *

import pygris

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

# Named dict linking FIPS to names
SFBA_LOOKUP = dict(zip(SFBA_FIPS, SFBA_NAMES))

# State FIPS and census year
CA_FIPS      = "06"
CENSUS_YEAR  = 2019

# SFBA census tract GEOIDs (loaded once here for use across scripts)
_tracts      = pygris.tracts(state=CA_FIPS, year=CENSUS_YEAR)
SFBA_GEOIDS  = _tracts[_tracts["COUNTYFP"].isin(SFBA_FIPS_3)]["GEOID"].tolist()


# ── File paths ────────────────────────────────────────────────────────────────

# Raw data inputs
SEER_FILE       = "data/raw/population_data/ca.1990_2024.singleages.through89.90plus.txt"
IHME_AC_FILE    = "data/raw/baseline_health_outcomes/IHME_USA_COD_COUNTY_RACE_ETHN_2000_2019_MX_2019_ALL_BOTH_Y2023M06D12.CSV"
CHIS_FILE       = "data/raw/baseline_health_outcomes/chis-data-current-asthma-prevalence-by-county-2015-present.csv"
PLACES_TRACT_FILE   = "data/raw/baseline_health_outcomes/PLACES__Census_Tract_Data__GIS_Friendly_Format___2024_release_20250313.csv"
PLACES_COUNTY_FILE  = "data/raw/baseline_health_outcomes/PLACES__County_Data__GIS_Friendly_Format___2024_release_20250312.csv"
USALEEP_FILE    = "data/raw/baseline_health_outcomes/BenMAP_Ready_USALEEP_AllCauseRates_2020.csv"
ALRI_FILE       = "data/raw/baseline_health_outcomes/ALRI_2019_CS3044.csv"
IHD_FILE        = "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC.csv"
LUNG_BOTH_FILE  = "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_BOTH_Y2025M06D15.CSV"
LUNG_MALE_FILE  = "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_MALE_Y2025M06D15.CSV"
LUNG_FEMALE_FILE = "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_FEMALE_Y2025M06D15.CSV"

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

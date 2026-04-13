# Snakefile
# BREATHE HIA pipeline orchestration
# Run with: snakemake --cores 1
# To force re-run a step: snakemake --cores 1 --forcerun seer_demographics

configfile: "config.py"

# ── Final target: running this builds everything ──────────────────────────────
rule all:
    input:
        "output/impact_results.parquet"

# ── Data preparation (scripts 1-9, run once) ──────────────────────────────────
rule seer_demographics:
    input:
        "data/raw/population_data/ca.1990_2024.singleages.through89.90plus.txt"
    output:
        "data/processed/pop_0_17_sex_race_county_2019.parquet"
    script:
        "src/data/seer_demographics.py"

rule chis_asthma:
    input:
        "data/raw/baseline_health_outcomes/chis-data-current-asthma-prevalence-by-county-2015-present.csv"
    output:
        "data/processed/asthma_prev_2021_2022.parquet"
    script:
        "src/data/chis_asthma.py"

rule cdc_places_asthma:
    input:
        "data/raw/baseline_health_outcomes/PLACES__Census_Tract_Data__GIS_Friendly_Format___2024_release_20250313.csv"
    output:
        "data/processed/cdc_places_asthma_2019.parquet"
    script:
        "src/data/cdc_places_asthma.py"

rule ihme_all_cause_mortality:
    input:
        "data/raw/baseline_health_outcomes/IHME_USA_COD_COUNTY_RACE_ETHN_2000_2019_MX_2019_ALL_BOTH_Y2023M06D12.CSV"
    output:
        "data/processed/all_cause_mortality_2019_ihme.parquet"
    script:
        "src/data/ihme_all_cause_mortality.py"

rule usaleep_mortality:
    input:
        "data/raw/baseline_health_outcomes/BenMAP_Ready_USALEEP_AllCauseRates_2020.csv"
    output:
        "data/processed/all_cause_mortality_2020_usaleep.parquet"
    script:
        "src/data/usaleep_mortality.py"

rule alri_hcai:
    input:
        "data/raw/baseline_health_outcomes/ALRI_2019_CS3044.csv"
    output:
        "data/processed/alri_2019_hcai.parquet"
    script:
        "src/data/alri_hcai.py"

rule ihd_mortality_cdc:
    input:
        "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC.csv"
    output:
        "data/processed/ihd_mortality_2019_cdc.parquet"
    script:
        "src/data/ihd_mortality_cdc.py"

rule lung_cancer_ihme:
    input:
        expand(
            "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_{sex}_Y2025M06D15.CSV",
            sex=["BOTH", "MALE", "FEMALE"]
        )
    output:
        "data/processed/lung_cancer_mortality_2019_ihme.parquet"
    script:
        "src/data/lung_cancer_ihme.py"

rule combine_baseline_health:
    input:
        "data/processed/pop_0_17_sex_race_county_2019.parquet",
        "data/processed/asthma_prev_2021_2022.parquet",
        "data/processed/cdc_places_asthma_2019.parquet",
        "data/processed/all_cause_mortality_2019_ihme.parquet",
        "data/processed/all_cause_mortality_2020_usaleep.parquet",
        "data/processed/alri_2019_hcai.parquet",
        "data/processed/ihd_mortality_2019_cdc.parquet",
        "data/processed/lung_cancer_mortality_2019_ihme.parquet"
    output:
        "data/processed/baseline_health_combined.parquet"
    script:
        "src/data/combine_baseline_health.py"

# ── Analysis (reruns when inputs change) ─────────────────────────────────────
rule prepare_exposure_population:
    input:
        "data/processed/baseline_health_combined.parquet",
        "data/raw/simulated_population/sfbay-tr_capacity_1_5-20230608_activitysim_data_persons.csv",
        "data/raw/exposure_data/sfbay-tr-discount-100-20230703_sfbay-tr_capacity_1_5-20230608_All_resultsISRM.csv"
    output:
        "data/processed/exposure_population_inputs.parquet"
    script:
        "src/analysis/prepare_exposure_population.py"

rule impact_analysis:
    input:
        "data/processed/exposure_population_inputs.parquet"
    output:
        "output/impact_results.parquet"
    script:
        "src/analysis/impact_analysis.py"

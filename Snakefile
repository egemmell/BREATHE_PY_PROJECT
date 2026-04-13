# Snakefile
# BREATHE HIA pipeline orchestration
# Run with: snakemake --cores 1
# To force re-run a step: snakemake --cores 1 --forcerun seer_demographics

# ── Final target: running this builds everything ──────────────────────────────
rule all:
    input:
        "output/hia_results_runs_long.parquet"

# ── Data preparation (scripts 1-9, run once) ──────────────────────────────────
rule seer_demographics:
    input:
        "data/raw/population_data/ca.1990_2024.singleages.through89.90plus.txt"
    output:
        "data/processed/pop_0_17_sex_race_county_2019.parquet"
    script:
        "src/data/seer_demographics.py"

rule chis_asthma:
    # downloads from CKAN API — no local input file required
    output:
        "data/processed/adult_currentasthma_state_county_2021_2022_CHIS.parquet",
        "data/processed/child_currentasthma_state_county_2021_2022_CHIS.parquet"
    script:
        "src/data/chis_asthma.py"

rule cdc_places_asthma:
    # downloads from CDC PLACES API — no local input file required
    output:
        "data/processed/adult_currentasthma_ctract_county_2019_CDCPlaces.parquet"
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
        "data/processed/ac_mortality_ct_tract_2020_USALEEP.parquet"
    script:
        "src/data/usaleep_mortality.py"

rule alri_hcai:
    input:
        "data/raw/baseline_health_outcomes/ALRI_2019_CS3044.csv",
        "data/processed/pop_0_17_sex_race_county_2019.parquet"
    output:
        "data/processed/alri_2019_hcai.parquet"
    script:
        "src/data/alri_hcai.py"

rule ihd_mortality_cdc:
    input:
        "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC_age.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC_age_sex.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_county_2019_CDC_all_ages.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_state_2019_CDC_age.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_state_2019_CDC_age_sex.csv",
        "data/raw/baseline_health_outcomes/ihd_mortality_state_2019_CDC_age_sex_race.csv"
    output:
        "data/processed/ihd_mortality_county_state_2019_CDC.parquet"
    script:
        "src/data/ihd_mortality_cdc.py"

rule lung_cancer_ihme:
    input:
        "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_BOTH_Y2025M06D15.CSV",
        "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_MALE_Y2025M06D15.CSV",
        "data/raw/baseline_health_outcomes/IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_FEMALE_Y2025M06D15.CSV"
    output:
        "data/processed/lung_cancer_mortality_2019_ihme.parquet"
    script:
        "src/data/lung_cancer_ihme.py"

rule combine_baseline_health:
    input:
        "data/processed/all_cause_mortality_2019_ihme.parquet",
        "data/processed/ac_mortality_ct_tract_2020_USALEEP.parquet",
        "data/processed/ihd_mortality_county_state_2019_CDC.parquet",
        "data/processed/lung_cancer_mortality_2019_ihme.parquet",
        "data/processed/adult_currentasthma_ctract_county_2019_CDCPlaces.parquet",
        "data/processed/adult_currentasthma_state_county_2021_2022_CHIS.parquet",
        "data/processed/child_currentasthma_state_county_2021_2022_CHIS.parquet",
        "data/processed/alri_2019_hcai.parquet"
    output:
        "data/processed/baseline_health_combined.parquet"
    script:
        "src/data/combine_baseline_health.py"

# ── Analysis (reruns when inputs change) ──────────────────────────────────────
rule prepare_exposure_population:
    input:
        "data/raw/simulated_population/sfbay-tr_capacity_1_5-20230608_activitysim_data_persons.csv",
        "data/raw/exposure_data/sfbay-tr-discount-100-20230703_sfbay-tr_capacity_1_5-20230608_All_resultsISRM.csv"
    output:
        "data/processed/exposure_population_inputs.parquet"
    script:
        "src/analysis/prepare_exposure_population.py"

rule impact_analysis:
    input:
        "data/processed/baseline_health_combined.parquet",
        "data/processed/exposure_population_inputs.parquet"
    output:
        "output/hia_results_runs_long.parquet",
        "output/hia_totals_by_run.parquet",
        "output/hia_by_age.parquet",
        "output/hia_by_geography.parquet",
        "output/hia_source_sensitivity.parquet",
        "output/hia_strata_sensitivity.parquet"
    script:
        "src/analysis/impact_analysis.py"

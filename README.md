# BREATHE_PY_PROJECT

**B**ay area **R**espiratory and **E**nvironmental **A**ir **T**ransport **H**ealth **E**valuation

A Health Impact Assessment (HIA) pipeline for the San Francisco Bay Area that estimates how changes in air pollutant concentrations (PM2.5, NO2, black carbon) from a transport scenario affect population health outcomes across demographic strata and geographies.

---

## Requirements

- [Miniconda or Miniforge](https://github.com/conda-forge/miniforge) (recommended: Miniforge)
- Git
- ~5 GB free disk space for raw data and processed outputs

---

## Setup

### 1. Clone the repository

```bash
git clone git@github.com:egemmell/BREATHE_PY_PROJECT.git
cd BREATHE_PY_PROJECT
```

### 2. Create the conda environment

```bash
conda env create -f environment.yml
conda activate breathe
```

### 3. Add raw data files

Some raw data files must be obtained separately and placed in the correct locations before running the pipeline. Contact the project team for access to these files.

The following files must be added to `data/raw/` before running:

**Population data** (`data/raw/population_data/`):
```
ca.1990_2024.singleages.through89.90plus.txt
```
Source: [SEER Cancer Institute unbridged population estimates](https://seer.cancer.gov/popdata/download.html) — download the California file for single-year ages.

**Baseline health outcomes** (`data/raw/baseline_health_outcomes/`):
```
IHME_USA_COD_COUNTY_RACE_ETHN_2000_2019_MX_2019_ALL_BOTH_Y2023M06D12.CSV
IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_BOTH_Y2025M06D15.CSV
IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_MALE_Y2025M06D15.CSV
IHME_USA_LUNG_CANCER_COUNTY_RACE_ETHNICITY_2000_2019_MX_2019_FEMALE_Y2025M06D15.CSV
BenMAP_Ready_USALEEP_AllCauseRates_2020.csv
ALRI_2019_CS3044.csv
ihd_mortality_county_2019_CDC.csv
ihd_mortality_county_2019_CDC_age.csv
ihd_mortality_county_2019_CDC_age_sex.csv
ihd_mortality_county_2019_CDC_all_ages.csv
ihd_mortality_state_2019_CDC_age.csv
ihd_mortality_state_2019_CDC_age_sex.csv
ihd_mortality_state_2019_CDC_age_sex_race.csv
```

**Simulated population** (`data/raw/simulated_population/`):
```
sfbay-tr_capacity_1_5-20230608_activitysim_data_persons.csv
```

**Exposure data** (`data/raw/exposure_data/`):
```
sfbay-tr-discount-100-20230703_sfbay-tr_capacity_1_5-20230608_All_resultsISRM.csv
```

> The following datasets are downloaded automatically from public APIs at runtime and do not need to be added manually:
> - CDC PLACES asthma prevalence (CDC PLACES API)
> - CHIS asthma prevalence (CKAN API)
> - US Census tract and county boundaries (pygris / US Census TIGER)

---

## Running the pipeline

### Run everything

From the project root with the `breathe` environment active:

```bash
snakemake --cores 1
```

Snakemake will automatically determine which steps need to run based on which output files are present and whether any inputs or scripts have changed since the last run. On first run, all 11 steps will execute. On subsequent runs, only steps with changed inputs will rerun.

### Preview what will run (dry run)

```bash
snakemake --cores 1 --dry-run
```

### Force rerun a specific step and everything downstream

```bash
snakemake --cores 1 --forcerun ihme_all_cause_mortality
```

### Visualize the pipeline dependency graph

```bash
snakemake --dag | dot -Tpng > docs/pipeline_dag.png
```

---

## Project structure

```
BREATHE_PY_PROJECT/
├── config.py               # Central config: FIPS codes, file paths, constants
├── Snakefile               # Pipeline orchestration
├── environment.yml         # Conda environment specification
│
├── src/
│   ├── data/               # Data cleaning scripts (steps 1–9)
│   │   ├── seer_demographics.py
│   │   ├── chis_asthma.py
│   │   ├── cdc_places_asthma.py
│   │   ├── ihme_all_cause_mortality.py
│   │   ├── usaleep_mortality.py
│   │   ├── alri_hcai.py
│   │   ├── ihd_mortality_cdc.py
│   │   ├── lung_cancer_ihme.py
│   │   └── combine_baseline_health.py
│   ├── analysis/           # Analysis scripts (steps 10–11)
│   │   ├── prepare_exposure_population.py
│   │   └── impact_analysis.py
│   └── utils/              # Shared helper functions
│       ├── geo_utils.py
│       └── io_utils.py
│
├── data/
│   ├── raw/                # Raw input data (not tracked by git — see Setup above)
│   └── processed/          # Cleaned outputs from steps 1–9 (not tracked by git)
│
├── notebooks/              # Exploratory analysis and QA checks
├── output/                 # Final HIA results (not tracked by git)
└── docs/
```

---

## Pipeline steps

| Step | Script | Description | Geography | Data source |
|------|--------|-------------|-----------|-------------|
| 1 | `seer_demographics.py` | County population by age, sex, race (0–17) | County | SEER |
| 2 | `chis_asthma.py` | Adult and child asthma prevalence | County, state | CHIS API |
| 3 | `cdc_places_asthma.py` | Adult asthma prevalence | Tract, county | CDC PLACES API |
| 4 | `ihme_all_cause_mortality.py` | All-cause mortality rates by age, sex, race | County | IHME |
| 5 | `usaleep_mortality.py` | All-cause mortality rates by age | Tract | USALEEP |
| 6 | `alri_hcai.py` | ALRI incidence in children 0–17 | County | HCAi |
| 7 | `ihd_mortality_cdc.py` | Ischemic heart disease mortality | County, state | CDC Wonder |
| 8 | `lung_cancer_ihme.py` | Lung cancer mortality by age, sex, race | County, state | IHME |
| 9 | `combine_baseline_health.py` | Combine all health outcome datasets | — | — |
| 10 | `prepare_exposure_population.py` | Spatial merge: simulated population × census tracts × ISRM exposure grid | Tract | ActivitySim / ISRM |
| 11 | `impact_analysis.py` | Log-linear HIA model: 26 runs across 6 outcomes, 3 pollutants | Tract, county, state | — |

---

## HIA model

The pipeline applies a log-linear concentration–response model:

```
y = m × P × (1 − exp(−β × Δx))
```

Where:
- `y` = attributable cases (deaths or prevalent cases)
- `m` = baseline rate per person
- `P` = population count per cell
- `β` = ln(RR) / concentration increment
- `Δx` = change in pollutant concentration

**Pollutants:** PM2.5, NO2, black carbon  
**Outcomes:** All-cause mortality, IHD mortality, lung cancer mortality, adult asthma, child asthma, ALRI in children  
**Uncertainty:** Dual propagation across RR confidence intervals and baseline rate confidence intervals

Results are written to `output/` as parquet files:

| File | Contents |
|------|----------|
| `hia_results_runs_long.parquet` | Full results, one row per population cell × run × exposure |
| `hia_totals_by_run.parquet` | Aggregated totals per run |
| `hia_by_age.parquet` | Results stratified by age group |
| `hia_by_geography.parquet` | Results aggregated by geography |
| `hia_source_sensitivity.parquet` | Source comparison for outcomes with multiple data sources |
| `hia_strata_sensitivity.parquet` | Stratification sensitivity comparison |

---

## Configuration

All FIPS codes, file paths, and constants are defined centrally in `config.py`. If any input file names or output paths change, update `config.py` — all scripts import from there.

---

## Notes

- Census tract and county boundary shapefiles are downloaded at runtime via `pygris` (US Census TIGER/Line 2019). No local shapefiles are required.
- CDC Wonder data (IHD mortality) cannot be downloaded via API at the county level due to [CDC API restrictions on location fields](https://wonder.cdc.gov/wonder/help/WONDER-API.html). These files must be obtained manually from CDC Wonder and shared with team members.
- The simulated exposure data (`NO2`, `BC`) in this version are randomly generated for pipeline testing. Replace with actual delta-x estimates in `prepare_exposure_population.py` when available.
- All processed outputs use the [parquet](https://parquet.apache.org/) format for efficient storage and fast read times. Use `pandas.read_parquet()` to load them in Python or `arrow::read_parquet()` in R.
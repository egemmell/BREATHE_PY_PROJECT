# BREATHE Project

Health Impact Assessment (HIA) pipeline for the San Francisco Bay Area.
Estimates how changes in air pollutant concentrations (PM2.5, NO2, black carbon)
from a transport scenario affect population health outcomes across demographic
strata and geographies.

## Setup

1. Clone the repository
2. Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) if not already installed
3. Create the conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate breathe
   ```
4. Add raw data files to `data/raw/` (see `DATAINFO.md` for sources)

## Running the pipeline

```bash
# Run everything (skips steps whose outputs already exist)
snakemake --cores 1

# Preview what would run without actually running it
snakemake --cores 1 --dry-run

# Visualize the pipeline dependency graph
snakemake --dag | dot -Tpng > docs/pipeline_dag.png

# Force re-run a specific step and anything downstream
snakemake --cores 1 --forcerun ihme_all_cause_mortality
```

## Project structure

```
├── config.py         # Central config: FIPS codes, file paths, constants
├── Snakefile         # Pipeline orchestration
├── environment.yml   # Conda environment spec
├── src/
│   ├── data/         # Data cleaning scripts (steps 1-9, run once)
│   ├── analysis/     # Analysis scripts (steps 10-11)
│   └── utils/        # Shared helper functions
├── data/
│   ├── raw/          # Raw input data (not tracked by git)
│   └── processed/    # Cleaned outputs (not tracked by git)
├── notebooks/        # Exploratory analysis and QA checks
└── output/           # Final results
```

## Pipeline steps

| Step | Script | Description |
|------|--------|-------------|
| 1 | `seer_demographics.py` | County population by age, sex, race (SEER) |
| 2 | `chis_asthma.py` | Adult asthma prevalence (CHIS 2021-22) |
| 3 | `cdc_places_asthma.py` | Adult asthma prevalence, tract-level (CDC PLACES) |
| 4 | `ihme_all_cause_mortality.py` | All-cause mortality rates (IHME 2019) |
| 5 | `usaleep_mortality.py` | All-cause mortality rates (USALEEP 2020) |
| 6 | `alri_hcai.py` | ALRI incidence (HCAi 2019) |
| 7 | `ihd_mortality_cdc.py` | Ischemic heart disease mortality (CDC 2019) |
| 8 | `lung_cancer_ihme.py` | Lung cancer mortality (IHME 2019) |
| 9 | `combine_baseline_health.py` | Combine all health outcome datasets |
| 10 | `prepare_exposure_population.py` | Spatial merge: population x exposure |
| 11 | `impact_analysis.py` | Log-linear HIA model |

# src/analysis/prepare_exposure_population.py
# Merge exposure grid data with simulated population residential locations
# and health outcome baseline data at census tract, county and state geographies
#
#   1. Load & clean simulated population
#   2. Load census tract boundaries via pygris
#   3. Spatially assign persons to census tracts
#   4. Load & reproject ISRM exposure grid
#   5. Spatially assign exposures to persons (residential point location)
#   6. Add sample NO2 and BC exposure data

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import CRS
import pygris
from config import CA_FIPS, SFBA_FIPS_3, CENSUS_YEAR, POP_FILE, ISRM_FILE, EXP_POP_OUT

# -----------------------------------------------------------------------------
# 1. Load and clean simulated population
# -----------------------------------------------------------------------------

print("Loading simulated population...")
pop = pd.read_csv(POP_FILE, usecols=["person_id", "age", "hispanic", "race_id",
                                      "sex", "household_id", "home_x", "home_y"])

# Recode race to IHME categories
# RAC1P codes: 1=White, 2=Black, 3-5=AIAN, 6=Asian, 7=NHPI, 8=Other, 9=Two or more
race_map = {
    "1": "White",
    "2": "Black",
    "3": "American Indian / Alaskan Native",
    "4": "American Indian / Alaskan Native",
    "5": "American Indian / Alaskan Native",
    "6": "Asian / Pacific Islander",
    "7": "Asian / Pacific Islander",
    "8": "Other",
    "9": "Other"
}
pop["race_name"] = pop["race_id"].astype(str).map(race_map)
pop["race_name"] = pop.apply(
    lambda r: "Hispanic" if r["hispanic"] == 1 else r["race_name"], axis=1
)
pop["sex_name"] = pop["sex"].astype(str).map({"1": "Male", "2": "Female"})
pop = pop.rename(columns={"age": "age_name"})
pop = pop[["person_id", "household_id", "age_name", "sex_name", "race_name",
           "home_x", "home_y"]]

# Convert to GeoDataFrame — population uses NAD83 (EPSG:4269)
pop = gpd.GeoDataFrame(
    pop,
    geometry=gpd.points_from_xy(pop["home_x"], pop["home_y"]),
    crs="EPSG:4269"
).drop(columns=["home_x", "home_y"])

print(f"Population loaded: {len(pop)} persons")

# -----------------------------------------------------------------------------
# 2. Load census tract boundaries via pygris
# -----------------------------------------------------------------------------

print("Loading census tract boundaries...")
ct = pygris.tracts(state=CA_FIPS, year=CENSUS_YEAR)
ct = ct[ct["COUNTYFP"].isin(SFBA_FIPS_3)][["GEOID", "geometry"]].rename(
    columns={"GEOID": "geoid"}
)
ct = ct.to_crs("EPSG:4269")
print(f"Census tracts loaded: {len(ct)}")

# -----------------------------------------------------------------------------
# 3. Spatially assign persons to census tracts (point in polygon)
# -----------------------------------------------------------------------------

print("Spatially joining persons to census tracts...")
popct = gpd.sjoin(pop, ct, how="inner", predicate="within").drop(
    columns=["index_right"]
)
popct = popct.to_crs("EPSG:3310")   # reproject to CA Albers

popct = popct.rename(columns={
    "person_id":    "ind_id",
    "household_id": "hsld_id",
    "age_name":     "age",
    "sex_name":     "sex",
    "race_name":    "race"
})
print(f"Persons assigned to tracts: {len(popct)}")

del pop, ct

# -----------------------------------------------------------------------------
# 4. Load and reproject ISRM exposure grid
# -----------------------------------------------------------------------------

print("Loading ISRM exposure grid...")

custom_wkt = '''PROJCRS["Lambert_Conformal_Conic",
  BASEGEOGCRS["GCS_unnamed ellipse",
    DATUM["unknown",
      ELLIPSOID["Unknown",6370997,0,
        LENGTHUNIT["metre",1,ID["EPSG",9001]]]],
    PRIMEM["Greenwich",0,
      ANGLEUNIT["Degree",0.0174532925199433]]],
  CONVERSION["unnamed",
    METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],
    PARAMETER["Latitude of false origin",40,
      ANGLEUNIT["Degree",0.0174532925199433],ID["EPSG",8821]],
    PARAMETER["Longitude of false origin",-97,
      ANGLEUNIT["Degree",0.0174532925199433],ID["EPSG",8822]],
    PARAMETER["Latitude of 1st standard parallel",33,
      ANGLEUNIT["Degree",0.0174532925199433],ID["EPSG",8823]],
    PARAMETER["Latitude of 2nd standard parallel",45,
      ANGLEUNIT["Degree",0.0174532925199433],ID["EPSG",8824]],
    PARAMETER["Easting at false origin",0,
      LENGTHUNIT["metre",1],ID["EPSG",8826]],
    PARAMETER["Northing at false origin",0,
      LENGTHUNIT["metre",1],ID["EPSG",8827]]],
  CS[Cartesian,2],
  AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],
  AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]'''

sim = pd.read_csv(ISRM_FILE)
sim = sim.rename(columns={"Unnamed: 0": "isrm"})
sim = gpd.GeoDataFrame(sim, geometry=gpd.GeoSeries.from_wkt(sim["geometry"]))
sim = sim.set_crs(CRS.from_wkt(custom_wkt))
sim = sim.to_crs("EPSG:3310")
print(f"ISRM grid cells loaded: {len(sim)}")

# -----------------------------------------------------------------------------
# 5. Spatially assign exposures to persons by residential location
# -----------------------------------------------------------------------------

print("Spatially joining persons to ISRM grid cells...")
exp = gpd.sjoin(popct, sim, how="inner", predicate="within").drop(
    columns=["index_right"]
)

# keep only needed columns
exp = exp[["geoid", "isrm", "ind_id", "hsld_id", "age", "sex", "race",
           "TotalPM25", "geometry"]]

print(f"Persons with exposure assigned: {len(exp)}")

del popct, sim

# -----------------------------------------------------------------------------
# 6. Add sample NO2 and BC exposure data
# -----------------------------------------------------------------------------
# NOTE: Replace with actual delta-x exposure estimates when available
# Ranges for SF Bay Area:
#   NO2: 6.3 to 84.6 ug/m3 | BC: 0.2 to 2.5 ug/m3
# Persons within same ISRM grid cell get same exposure change

print("Generating sample NO2 and BC exposures...")
np.random.seed(42)

isrm_cells = exp[["isrm"]].drop_duplicates().copy()
isrm_cells["NO2"] = np.random.uniform(0.1,  28.3, size=len(isrm_cells))
isrm_cells["BC"]  = np.random.uniform(0.01,  2.3, size=len(isrm_cells))

exp = exp.merge(isrm_cells, on="isrm", how="left")

# negative sign = reduction in exposure
exp["NO2"] = -exp["NO2"]
exp["BC"]  = -exp["BC"]

# -----------------------------------------------------------------------------
# QA checks
# -----------------------------------------------------------------------------

print(f"\nNO2 range: {exp['NO2'].min():.3f} to {exp['NO2'].max():.3f}")
print(f"BC range:  {exp['BC'].min():.3f} to {exp['BC'].max():.3f}")
print(f"Persons with NO2 assigned: {exp['NO2'].notna().sum()} of {len(exp)} "
      f"({100 * exp['NO2'].notna().mean():.1f}%)")
print(f"Persons with BC assigned:  {exp['BC'].notna().sum()} of {len(exp)} "
      f"({100 * exp['BC'].notna().mean():.1f}%)")

# -----------------------------------------------------------------------------
# Save (drop geometry for parquet)
# -----------------------------------------------------------------------------

exp.drop(columns=["geometry"]).to_parquet(EXP_POP_OUT, index=False)
print(f"\nSaved {len(exp)} rows to {EXP_POP_OUT}")
# src/data/seer_demographics.py
# Download and clean US Census county-level population data for single year ages, race/ethnicity
# Source: SEER Cancer Institute unbridged population estimates (2019)
# https://seer.cancer.gov/popdata/download.html
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import pygris
from config import SFBA_FIPS, CA_FIPS, CENSUS_YEAR, SEER_FILE, SEER_OUT

# rebuild chunks
chunks = []
for i, chunk in enumerate(pd.read_csv(SEER_FILE, sep='\t', header=None, dtype=str, chunksize=5000)):
    chunk = chunk.iloc[:, 0]
    df = pd.DataFrame({
        'year':       chunk.str[0:4],
        'st_fips':    chunk.str[6:8],
        'co_fips':    chunk.str[8:11],
        'race':       chunk.str[13:14],
        'hispanic':   chunk.str[14:15],
        'sex_name':   chunk.str[15:16],
        'age_name':   chunk.str[16:18].astype(int),
        'population': chunk.str[18:26].astype(float),
    })
    df['fips'] = df['st_fips'] + df['co_fips']
    df = df[(df['year'] == '2019') & (df['age_name'] < 18) & (df['fips'].isin(SFBA_FIPS))]
    if not df.empty:
        chunks.append(df[['fips', 'age_name', 'sex_name', 'hispanic', 'race', 'year', 'population']])

pop = pd.concat(chunks, ignore_index=True)
print('Step 1 - concat OK:', len(pop), 'rows')

ca_counties = pygris.counties(state=CA_FIPS, year=CENSUS_YEAR)
ca_counties = ca_counties[ca_counties['GEOID'].isin(SFBA_FIPS)][['GEOID', 'NAME']]
fips_lookup = ca_counties.rename(columns={'GEOID': 'fips', 'NAME': 'location_name'})
print('Step 2 - fips_lookup OK:', len(fips_lookup), 'rows')

race_map = {'1': 'White', '2': 'Black',
            '3': 'American Indian / Alaskan Native',
            '4': 'Asian / Pacific Islander'}
pop['race_name'] = pop['race'].map(race_map)
pop.loc[pop['hispanic'] == '1', 'race_name'] = 'Hispanic'
pop = pop.drop(columns=['race', 'hispanic'])
print('Step 3 - recode OK:', len(pop), 'rows')

pop = fips_lookup.merge(pop, on='fips', how='left')
print('Step 4 - merge OK:', len(pop), 'rows')

pop['sex_name'] = pop['sex_name'].map({'1': 'Male', '2': 'Female'})
race_total = (pop.groupby(['fips', 'location_name', 'age_name', 'sex_name'], as_index=False)
                 ['population'].sum()
                 .assign(race_name='Total', year='2019'))
print('Step 5 - race_total OK:', len(race_total), 'rows')

both_sexes = (pd.concat([pop, race_total])
                .groupby(['fips', 'location_name', 'age_name', 'race_name'], as_index=False)
                ['population'].sum()
                .assign(sex_name='Both', year='2019'))
print('Step 6 - both_sexes OK:', len(both_sexes), 'rows')

pop = pd.concat([pop, race_total, both_sexes], ignore_index=True)
pop = (pop.groupby(['fips', 'location_name', 'race_name', 'sex_name', 'year'], as_index=False)
          ['population'].sum()
          .assign(age_name='0-17'))
pop = pop.rename(columns={
    "fips":          "geoid",
    "location_name": "lctn_nm",
    "race_name":     "race_grp",
    "sex_name":      "sex_grp"
})
print('Step 7 - final pop OK:', len(pop), 'rows')

pop.to_parquet(SEER_OUT, index=False)
print('Step 8 - saved to', SEER_OUT)
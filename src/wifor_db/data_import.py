"""
This script saves data  to a SQL database. 

Run for example with:
poetry run python src/wifor_db/data_import.py
"""

import pandas as pd
import geopandas as gpd
import eurostat
from wifor_db import TABLE_CONNECTOR

geo_df = gpd.read_file('../geo_data/ref-nuts-2021/NUTS_RG_01M_2021_4326.geojson')
geo_df.rename(columns={'NUTS_ID': 'nuts_id'
                       , 'LEVL_CODE': 'levl_code'
                       , 'CNTR_CODE': 'cntr_code'
                       , 'NAME_LATN': 'name_latin'
                       , 'NUTS_NAME': 'nuts_name'
                       , 'MOUNT_TYPE': 'mount_type'
                       , 'URBN_TYPE': 'urban_type'
                       , 'COAST_TYPE': 'coast_type'
                       , 'FID': 'fid'
                       }
                , inplace=True)

with TABLE_CONNECTOR() as tc:
    regions = tc.open_table("REGIONS")
    regions.init_table()
    regions.add_data(geo_df)

# Employment by sex, age and economic activity (from 2008 onwards, NACE Rev. 2) - 1 000
# https://ec.europa.eu/eurostat/web/products-datasets/product?code=lfsq_egan2

data1 = eurostat.get_data_df("lfsa_egan2", False) # Zeit, Land, Geschlecht, Alter, NACE 2

data1.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data1 = data1.melt(id_vars=['freq', 'unit', 'sex', 'age', 'nace_r2', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data1['year'] = pd.to_datetime(data1['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_egan2 = tc.open_table("lfsa_egan2")
    lfsa_egan2.init_table()
    lfsa_egan2.add_data(data1)

# Employment rates by sex, age and citizenship (%)
# https://ec.europa.eu/eurostat/web/products-datasets/-/lfsa_ergan

data2 = eurostat.get_data_df("lfsa_egan", False) # Zeit, Land, Geschlecht, Nationalität, Alter

data2.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data2 = data2.melt(id_vars=['freq', 'unit', 'sex', 'age', 'citizen', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data2['year'] = pd.to_datetime(data2['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_egan = tc.open_table("lfsa_egan")
    lfsa_egan.init_table()
    lfsa_egan.add_data(data2)

# Employment by sex, age, occupation and economic activity (from 2008 onwards, NACE Rev. 2) (1 000)
# https://ec.europa.eu/eurostat/web/products-datasets/-/lfsa_eisn2

data3 = eurostat.get_data_df("lfsa_eisn2", False) # Zeit, Land, ISCO1, NACE2, Geschlecht, Alter

data3.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data3 = data3.melt(id_vars=['freq', 'age', 'sex', 'nace_r2', 'isco08', 'unit', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data3['year'] = pd.to_datetime(data3['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_eisn2 = tc.open_table("lfsa_eisn2")
    lfsa_eisn2.init_table()
    lfsa_eisn2.add_data(data3)

# Employed persons by detailed occupation (ISCO-08 two digit level)
# https://ec.europa.eu/eurostat/web/products-datasets/-/lfsa_egai2d

data4 = eurostat.get_data_df("lfsa_egai2d", False) # Zeit, Land, ISCO2, Geschlecht

data4.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data4 = data4.melt(id_vars=['freq', 'isco08', 'age', 'sex', 'unit', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data4['year'] = pd.to_datetime(data4['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_egai2d = tc.open_table("lfsa_egai2d")
    lfsa_egai2d.init_table()
    lfsa_egai2d.add_data(data4)

# Unemployment by sex, age and duration of unemployment (1 000)
# https://ec.europa.eu/eurostat/web/products-datasets/-/lfsa_ugad

data5 = eurostat.get_data_df("lfsa_ugad", False) # Zeit, Land, Geschlecht, Dauer Alo, Alter

data5.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data5 = data5.melt(id_vars=['freq', 'unit', 'sex', 'age', 'duration', 'nuts_id'], 
                  var_name='year', 
                  value_name='unemployed')
data5['year'] = pd.to_datetime(data5['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_ugad = tc.open_table("lfsa_ugad")
    lfsa_ugad.init_table()
    lfsa_ugad.add_data(data5)

# Previous occupations of the unemployed, by sex (1 000)
# https://ec.europa.eu/eurostat/web/products-datasets/product?code=lfsa_ugpis

data6 = eurostat.get_data_df("lfsa_ugpis", False) # Zeit, Land, Geschlecht, ISCO1 Alo

data6.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data6 = data6.melt(id_vars=['freq', 'unit', 'sex', 'isco08', 'nuts_id'], 
                  var_name='year', 
                  value_name='unemployed')
data6['year'] = pd.to_datetime(data6['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_ugpis = tc.open_table("lfsa_ugpis")
    lfsa_ugpis.init_table()
    lfsa_ugpis.add_data(data6)

# Employment by sex, age, economic activity and NUTS 2 regions (NACE Rev. 2) (1 000)
# https://ec.europa.eu/eurostat/web/products-datasets/-/LFST_R_LFE2EN2

data7 = eurostat.get_data_df("lfst_r_lfe2en2", False) # Region NUTS2, Zeit, NACE2, Alter, Geschlecht
data7.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data7 = data7.melt(id_vars=['freq', 'nace_r2', 'age', 'sex', 'unit', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data7['year'] = pd.to_datetime(data7['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfst_r_lfe2en2 = tc.open_table("lfst_r_lfe2en2")
    lfst_r_lfe2en2.init_table()
    lfst_r_lfe2en2.add_data(data7)

# Employment by sex, age, migration status, occupation and educational attainment level
# https://ec.europa.eu/eurostat/web/products-datasets/-/lfsa_egaisedm

data8 = eurostat.get_data_df("lfsa_egaisedm") # Beschäftigung nach Geschlecht, Alter, Migrationsstatus, Beruf und Bildungsabschluss

data8.rename(columns={'geo\\TIME_PERIOD': 'nuts_id'}, inplace=True)
data8 = data8.melt(id_vars=['freq', 'isced11', 'isco08', 'mgstatus', 'age', 'sex', 'unit', 'nuts_id'], 
                  var_name='year', 
                  value_name='employed')
data8['year'] = pd.to_datetime(data8['year'], format='%Y')

with TABLE_CONNECTOR() as tc:
    lfsa_egaisedm = tc.open_table("lfsa_egaisedm")
    lfsa_egaisedm.init_table()
    lfsa_egaisedm.add_data(data8)

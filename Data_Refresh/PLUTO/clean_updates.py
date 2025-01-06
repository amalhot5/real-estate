import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, text
import numpy as np
from sqlalchemy.orm import sessionmaker
import geopandas as gpd

import pickle as pkl

from datetime import datetime

from building_refresh_v2 import get_pw_data

def get_mn_ids(preso, borough):
    try:
        with open('data/pluto_map.pkl', 'rb') as f:
            pluto = pkl.load(f)
    except FileNotFoundError:
        shapefile_path = 'data/nyc_mappluto_23v1_1_shp/MapPLUTO.shp'
        gdf = gpd.read_file(shapefile_path)
        pluto = pluto[pluto['bbl'].isin(gdf['BBL'].to_list())]
        with open('data/pluto_map.pkl', 'wb') as f:
            pkl.dump(pluto, f)
        # Print GeoDataFrame information
        print(gdf.head())

    preso['source_id'] = pd.to_numeric(preso['source_id'])
    matched_buildings = preso.merge(pluto, how='inner', left_on='source_id', right_on='bbl')
    return matched_buildings[matched_buildings['borough'] == borough]['id']
    
def get_percent_change(old_value, new_value):
    try:
        return abs(float(old_value) - float(new_value)) / float(old_value)
    except ZeroDivisionError:
        return 1
def clean_updates(field_name, old_value, new_value):
    '''if field_name in ('building_class', 'landmark_x', 'zip', 'geometry_x'):
        return True
    else:
        return old_value == 0'''
    match field_name:
        case 'building_area':
            return get_percent_change(old_value, new_value) >= 0.05
        case 'year_built':
            return float(new_value) - float(old_value) >= 5
        case 'lot_area':
            return get_percent_change(old_value, new_value) >= 0.1
        case 'lot_depth':
            return get_percent_change(old_value, new_value) >= 0.1
        case 'lot_front':
            return get_percent_change(old_value, new_value) >= 0.1
        case 'num_units':
            return old_value == 0
        case _:
            return True
        
def main(borough):
    update_df = pd.read_csv('data/update_df.csv')
    
    keep = [clean_updates(x, y, z) for x, y, z in zip(update_df['field_name'], update_df['old_value'], update_df['new_value'])]
    update_df = update_df[keep]

    preso = get_pw_data('pw_query.sql')
    
    update_class = update_df[update_df['field_name'] == 'building_class']
    resbuildings = set(update_class[(update_class['old_value'].str[0].isin(['A', 'R', 'B', 'C', 'D', 'L', 'S'])) | (update_class['new_value'].str[0].isin(['A', 'R', 'B', 'C', 'D', 'L', 'S']))]['id'])
    res_ids = set(preso[(preso['building_class'].str[0].isin(['A', 'R', 'B', 'C', 'D', 'L', 'S']))]['id'])
    all_res_ids = set(update_df[(update_df['id'].isin(resbuildings)) | (update_df['id'].isin(res_ids))]['id'])
    update_df = update_df[update_df['id'].isin(all_res_ids)]

    mn_ids = get_mn_ids(preso, borough)
    update_df = update_df[update_df['id'].isin(mn_ids)]

    not_geo = set(update_df[update_df['field_name'] != 'geometry_x']['id'])
    update_df = update_df[update_df['id'].isin(not_geo)]
    update_df['field_name'] = update_df['field_name'].replace({'geometry_x': 'geometry', 'landmark_x': 'landmark'})

    update_df[['id', 'field_name', 'old_value', 'new_value']].to_csv(f'updates_{borough}1.csv', index=False)
    print(update_df.groupby('field_name')['id'].nunique())
if __name__ == '__main__':
    main('MN')

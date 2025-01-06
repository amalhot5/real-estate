import pandas as pd
import geopandas as gpd
import re
from shapely.wkt import loads
from shapely.geometry import shape
import json
import pickle as pkl

def load_updates(fname):
    updates = pd.read_csv(fname)
    update_geos = updates[updates['field_name'].str.contains('geometry')]
    return set(updates['id'].values), update_geos

def load_df(fname):
    df = pd.read_csv(fname)
    return df

def load_shape(x):
    try:
        return loads(x)
    except:
        return shape(json.loads(x))

def load_geos(df: pd.DataFrame):
    preso_gdf = gpd.GeoDataFrame(df[(df.geometry.notnull()) | (df.geometry != None)])
    preso_gdf['geometry'] = [load_shape(x) for x in preso_gdf['geometry']]    
    preso_gdf = preso_gdf.set_geometry('geometry')
    preso_gdf.set_crs(crs="EPSG:4326", inplace=True)
    return preso_gdf

def load_pluto_gdf(df, shp_fpath=None):
    try:
        with open('data/gdf.pkl', 'rb') as f:
            pluto_gdf = pkl.load(f)
    except FileNotFoundError:
        pluto_gdf = gpd.read_file(shp_fpath)
        pluto_gdf.to_crs('epsg:4326', inplace=True)
    create_ids = set(df['bbl'])
    return pluto_gdf[pluto_gdf['BBL'].isin(create_ids)]

def geo_overlap(hides_df, other_df):
    hides_gdf = load_geos(hides_df[['id', 'geometry']])
    try:
        other_gdf = other_df[['id', 'new_value']]
    except KeyError:
        print(other_df.columns)
        other_gdf = load_pluto_gdf(set(other_df['BBL'].values()))
    other_gdf.columns = ['id', 'geometry']
    other_gdf = load_geos(other_gdf)
    sp1 = gpd.sjoin(hides_gdf, other_gdf, op='intersects')
    return sp1

def address_match(other, manually_created):
    print(other.head())
    pluto_buildings = [x.upper() for x in other['display_address']]
    manually_created['display_address'] = [x.upper() for x in manually_created['display_address']]
    pluto_buildings = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x)\
                                for x in pluto_buildings]
    
    pluto_buildings = [re.sub('-', '', x) for x in pluto_buildings]
    manually_created['display_address'] = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x)\
                                          for x in manually_created['display_address']]
    
    pluto_buildings_match = manually_created[manually_created['display_address'].isin(pluto_buildings)]
    return pluto_buildings_match

def check_hides(hide_df: pd.DataFrame, create_df: pd.DataFrame, update_ids: set, update_geos: pd.DataFrame) -> pd.DataFrame:
    # check overlap with building_id
    # check overlap with geometry (intersect)
    # address match with create_df
    # create_df will not have building_id
    hide = hide_df[hide_df['id'].isin(update_ids)]
    hide4 = address_match(create_df, hide_df)
    print(hide.shape, hide4.shape)
    hide2 = geo_overlap(hide_df, update_geos)
    create_gdf = load_pluto_gdf(create_df)
    hide3 = geo_overlap(hide_df, create_gdf)
    
    
    print(hide_df.shape, hide.shape, hide2.shape, hide3.shape, hide4.shape)

def main():
    update_ids, update_geos = load_updates('data/update_df.csv')
    creates = load_df('data/final_create_buildings.csv')
    hides = load_df('data/final_deprecate.csv')
    check_hides(hides, creates, update_ids, update_geos)
    

if __name__ == '__main__':
    main()
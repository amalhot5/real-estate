"""
    This script is used to update the data in Perchwell's building table
    with the latest PLUTO version
    
    Author(s): Arnav Malhotra
    Created On: 2023-07-06
    Updated On:
    Reviewer:
"""
import pickle as pkl
import pandas as pd
import sqlalchemy as db
import numpy as np
import geopandas as gpd
import json
import re

from sqlalchemy import create_engine, text
from shapely.geometry import shape
from shapely.geometry.multipolygon import MultiPolygon
from shapely import to_geojson
from sqlalchemy.orm import sessionmaker
from datetime import datetime


def pull_data(sql_query:str, file_out=None, port_num=2023):
    """
    For a given sql_query, this function will connect to Perchwell's database, pull
    the corresponding data, save it to a csv in the file specified (default is 
    pw.csv), and returns that data in the form of a Pandas DataFrame.

    Args:
        sql_query (str): query whose results you wish to download
        file_out (str, optional): filepath for the output file. If None, then
            no file is created. Defaults to None.
        port_num (int, optional): Port number for Teleport Connection. Defaults
            to 2023.

    Returns:
        pd.DataFrame: DataFrame containing results of sql_query 
    """    
    if sql_query[-1] == ';':
        sql_query = sql_query[:-1]
    pw_df = []
    prev_row_count = -1
    current_row = 0
    # Create the SQLAlchemy engine
    engine = create_engine(\
        f"postgresql://teleport:@localhost:{port_num}/perchwell")

    # in order to get around the 300k row limit
    while prev_row_count <= current_row:
        current_row = len(pw_df)
        if prev_row_count == current_row:
            print(f"final row count: {current_row}")
            break
        # Define the SQL query string, but keep last line
        query_string = f"""{sql_query}
            -- NOTE: DO NOT DELETE BELOW THIS
            LIMIT 100000 OFFSET {current_row};
            """
        try:
            # Execute the query
            with engine.connect() as conn:
                result = conn.execute(text(query_string))

                # Process the query result
                for row in result:
                    # Access the row data
                    pw_df.append(row)
        except:
            # in case of timeout
            engine = create_engine(\
                f"postgresql://teleport:@localhost:{port_num}/perchwell")
            with engine.connect() as conn:
                result = conn.execute(text(query_string))
                for row in result:
                    pw_df.append(row)
        print(f"{len(pw_df)} rows appended")
        prev_row_count = current_row
    pw_df = pd.DataFrame(pw_df)
    if file_out:
        pw_df.to_csv(file_out)
    return pw_df

def get_pw_data(fpath):
    print('starting to get pw_data')
    try:
        with open('data/buildings_final3.pkl', 'rb') as f:
            preso = pkl.load(f)
        return preso
    except FileNotFoundError:
        with open(fpath, 'r') as query_file:
            query_string = query_file.read()
        preso = pull_data(query_string)
        with open('data/buildings_final3.pkl', 'wb') as f:
            pkl.dump(preso, f)
        return preso

def pw_to_gdf(preso):
    preso_gdf = gpd.GeoDataFrame(preso[preso.geometry_json.notnull()])
    preso_gdf['geometry'] = [shape(json.loads(x)) for x in preso_gdf['geometry_json']]    
    preso_gdf = preso_gdf.set_geometry('geometry')
    preso_gdf.set_crs(crs="EPSG:4326", inplace=True)
    preso_gdf['num_rls'].fillna(0, inplace=True)
    preso_gdf['num_mlsli'].fillna(0, inplace=True)
    preso_gdf['num_other_listings'].fillna(0, inplace=True)
    preso_gdf['num_acris'].fillna(0, inplace=True)
    return preso_gdf

def get_pluto(pluto_fpath, shp_fpath):
    print('loading pluto_gdf')
    try:
        with open('data/gdf.pkl', 'rb') as f:
            pluto_gdf = pkl.load(f)
    except FileNotFoundError:
        pluto_gdf = gpd.read_file(shp_fpath)
        pluto_gdf.to_crs('epsg:4326', inplace=True)
    print('loading pluto files')
    if pluto_fpath:
        pluto = pd.read_csv(pluto_fpath)
        pluto = pluto[pluto['bbl'].isin(pluto_gdf['BBL'].to_list())]
    else:
        pluto = None
    return pluto, pluto_gdf

def update_buildings(matched_buildings, mapped_fields):
    update_df = {'id': [], 'field_name': [], 'old_value': [],
             'new_value': [], 'in_search': []}
    for fnames in mapped_fields:
        wrong = matched_buildings[matched_buildings[f'{fnames[0]}'] !=\
                                   matched_buildings[f'{fnames[1]}']]
        wrong = wrong[(wrong[f'{fnames[1]}'] != 0)]
        wrong = wrong[~(wrong[f'{fnames[1]}'].isna())]
        if fnames[0] in ('num_stories', 'year_built'):
            # new value should be greater for both year_built and num_stories
            wrong = wrong[wrong[f'{fnames[0]}'] < wrong[f'{fnames[1]}']]

        update_df['id'] += list(wrong['id'])
        update_df['old_value'] += list(wrong[f'{fnames[0]}'])
        update_df['new_value'] += list(wrong[f'{fnames[1]}'])
        update_df['in_search'] += list(wrong['in_search'])
        update_df['field_name'] += [f'{fnames[0]}'] * len(wrong['id'])
    
    update_df = pd.DataFrame(update_df)
    return update_df

def bbl_match(pw_df, pluto_df):
    pw_df['source_id'] = pd.to_numeric(pw_df['source_id'], errors='coerce')
    matched_buildings = pw_df.merge(pluto_df, how='inner', left_on='source_id',\
                                     right_on='bbl')
    
    matched_buildings['zip'] = pd.to_numeric(matched_buildings['zip'],\
                                              errors='coerce')
    
    matched_buildings['numfloors'] = np.ceil(matched_buildings['numfloors'])
    matched_buildings['landmark_y'] = [True if type(x) == str else False for\
                                        x in matched_buildings['landmark_y']]
    
    matched_buildings['landmark_x'] = [x if x else 'f' for x in\
                                        matched_buildings['landmark_x']]
    
    matched_buildings['landmark_x'] = [False if \
                                       (x.lower() in ('f', 'false') or\
                                        x.isspace()) else True \
                                        for x in matched_buildings['landmark_x']]
    return matched_buildings

def pw_only(pw_only):
    pw_only = pw_only[pw_only['in_search'] == True]
    manual_pluto = pw_only[(pw_only['source'].str.contains('manual')) | \
                           (pw_only['source'].str.contains('pluto'))]
    pitney_bowes = pw_only[~pw_only['id'].isin(manual_pluto['id'])]

    no_listings = manual_pluto[(manual_pluto['num_acris'] + \
                     manual_pluto['num_mlsli'] + \
                     manual_pluto['num_other_listings'] + \
                     manual_pluto['num_rls']) == 0]
    has_listings = manual_pluto[~manual_pluto['id'].isin(no_listings['id'])]
    
    from_pluto = has_listings[has_listings['source'].str.contains('pluto')]
    from_pluto['last_acris_sale'] = pd.to_datetime(\
            from_pluto['last_acris_sale'], format='%Y-%m-%d', errors='coerce')
    from_pluto_post2017 = from_pluto[(from_pluto['last_acris_sale'] >= \
                                      datetime.fromisoformat('2017-01-01')) |\
                                     (from_pluto['last_listing'] >= \
                                      datetime.fromisoformat('2017-01-01')) |\
                                     (from_pluto['last_mlsli_listing'] >=\
                                      datetime.fromisoformat('2017-01-01'))]
    pluto_pre2017 = from_pluto[~(from_pluto['id'].isin(from_pluto_post2017['id']))]
    
    manually_created = has_listings[has_listings['source'].str.contains('manual')]
    for_mlsli = manually_created[(manually_created['num_mlsli'] > 0) & \
                                 (manually_created['num_rls'] == 0)]
    for_rls = manually_created[(manually_created['num_mlsli'] == 0) & \
                               (manually_created['num_rls'] > 0)]
    for_other = manually_created[~(manually_created['id'].isin(for_mlsli['id']))\
                                  & ~(manually_created['id'].isin(for_rls['id']))]
    
    return pitney_bowes, no_listings, from_pluto_post2017, pluto_pre2017, for_mlsli, for_rls, for_other

def pluto_get_resbuildings(gdf):
    in_pw_area = gdf[(gdf['borough'] != 'SI') & (gdf['longitude'] < -73.74376451)]
    return in_pw_area[in_pw_area['resarea'] > 0]

def pluto_address_match(resbuildings, manually_created):
    resbuildings['address'] = [x.upper() for x in resbuildings['address']]
    manually_created['display_address'] = [x.upper() for x in manually_created['display_address']]
    resbuildings['address'] = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x)\
                                for x in resbuildings['address']]
    
    resbuildings['address'] = [re.sub('-', '', x) for x in resbuildings['address']]
    manually_created['display_address'] = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x)\
                                          for x in manually_created['display_address']]
    
    resbuildings_match = manually_created.merge(resbuildings, how='inner', left_on='display_address', right_on='address')
    resbuildings_no_match = resbuildings[~resbuildings['bbl'].isin(resbuildings_match['bbl'])]
    return resbuildings_match, resbuildings_no_match

def pluto_pw_plot_join(pluto_gdf, preso_gdf):
    sp1 = gpd.sjoin(preso_gdf, pluto_gdf, op='intersects')
    sp1_hidden = sp1[sp1['in_search'] != True]
    sp1_shown = sp1[sp1['in_search'] == True]
    not_found = pluto_gdf[~pluto_gdf['bbl'].isin(sp1['bbl'])]
    return sp1_shown, sp1_hidden, not_found

def update_pluto_only(pluto_gdf, pw_manually_created, preso):
    resbuildings = pluto_get_resbuildings(pluto_gdf)
    address_match, no_match = pluto_address_match(resbuildings, pw_manually_created)

    preso_gdf = preso.drop(columns=['created_at',
                            'updated_at',
                            'new_dev_start_date',
                            'new_dev_end_date',
                            'land_lease_expiration',
                            'last_rls_listing',
                            'last_mlsli_listing',
                            'last_listing',
                            'last_acris_created'])
    geo_joined_shown, geo_joined_hidden, not_found = pluto_pw_plot_join(no_match, preso_gdf)
    
    return address_match, geo_joined_shown, geo_joined_hidden, not_found

def create_shap_file(gdf, dir_out):
    try:
        gdf[['bbl', 'geometry', 'address', 'zipcode']].to_file(f'data/{dir_out}')
    except KeyError:
        try:
            preso_gdf = gpd.GeoDataFrame(gdf[gdf.geometry_json.notnull()])
            preso_gdf['geometry'] = [shape(json.loads(x)) for x in preso_gdf['geometry_json']]    
            preso_gdf = preso_gdf.set_geometry('geometry')
            preso_gdf.set_crs(crs="EPSG:4326", inplace=True)
            preso_gdf[['id', 'geometry', 'display_address', 'zip', 'source_id']].to_file(f'data/{dir_out}')
        except ValueError:
            print(f'{dir_out} is empty')

def convert_geos(geo, fname):
    if 'geometry' in fname:
        try:
            return to_geojson(MultiPolygon([geo]))
        except ValueError:
            return to_geojson(geo)
    else:
        return geo
    
def update_amenities(matched_buildings, amenities):
    # Update preso buildings 'Elevator'
    elevator_buildings_ids = matched_buildings[(matched_buildings['bldgclass'].notnull()) & (matched_buildings['bldgclass'].str.startswith(tuple(['D','R4'])))].id.tolist()
    amenities[amenities['amenities_id'].isin(elevator_buildings_ids)]['elevator'] = True
    
    # Update preso buildings 'New Development'
    newdevs_ids = matched_buildings[(matched_buildings.year_built < matched_buildings.yearbuilt) 
                                    & (matched_buildings.yearbuilt >= 2008) # New year built >= 2008, -15 from current year
                                    & (matched_buildings['bldgclass'].str.startswith(tuple(['A','B','C','R','D','S'])))].id.tolist() # Select only residential buildings
    amenities[amenities['amenities_id'].isin(newdevs_ids) | (amenities.sponsored == True)]['new_development'] = True

    # Update preso buildings 'Garage'
    garagearea_ids = matched_buildings[matched_buildings['garagearea'] > 0 ].id.tolist()
    amenities[amenities['amenities_id'].isin(garagearea_ids)]['garage'] = True
    all_update_ids = set(elevator_buildings_ids + newdevs_ids + garagearea_ids)
    
    return amenities[amenities['amenities_id'].isin(all_update_ids)][['amenities_id','garage','new_development', 'elevator']]
def main():
    # getting PW and PLUTO files
    preso = get_pw_data('pw_query.sql')
    preso = pw_to_gdf(preso)
    pluto, pluto_gdf = get_pluto(None, 'data/nyc_mappluto_23v1_1_shp/MapPLUTO.shp')
    pluto_gdf.columns = [c.lower() for c in pluto_gdf.columns]
    print('done loading data')
    # BBL Matching
    matched_buildings = bbl_match(preso, pluto_gdf)
    mapped_fields = [('zip', 'zipcode'),
                    ('year_built', 'yearbuilt'),
                    #('school_district_code', 'schooldist'),
                    ('num_stories', 'numfloors'), 
                    ('num_units', 'unitsres'), 
                    ('lot_area', 'lotarea'), 
                    ('lot_front', 'lotfront'), 
                    ('lot_depth', 'lotdepth'),
                    ('building_class', 'bldgclass'),
                    ('building_front', 'bldgfront'),
                    ('building_depth', 'bldgdepth'),
                    ('building_area', 'bldgarea'),
                    ('geometry_x', 'geometry_y'),
                    ('landmark_x', 'landmark_y')]
    update_df = update_buildings(matched_buildings, mapped_fields)
    update_df['new_value'] = [convert_geos(x, field) for x, field in zip(update_df['new_value'], update_df['field_name'])]
    print(f'completed BBL match and first update: {update_df["id"].nunique()}')

    # dealing with PW only buildings
    pitney_bowes, no_listings, pluto_post2017, pluto_pre2017, for_mlsli, for_rls, for_other =\
        pw_only(preso[~(preso['id'].isin(matched_buildings['id']))])
    deprecate = pd.concat([no_listings, pluto_pre2017])
    deprecate.to_csv('data/final_deprecate.csv')
    for_mlsli.to_csv('data/final_manual_review_mlsli.csv')
    for_rls.to_csv('data/final_manual_review_rls.csv')
    for_other.to_csv('data/final_manual_review_other.csv')
    pluto_post2017.to_csv('data/final_manual_review_pluto_17.csv')
    create_shap_file(for_mlsli, 'manual_review_mlsli')
    create_shap_file(for_rls, 'manual_rls')
    create_shap_file(for_other, 'manual_review_other')
    create_shap_file(pluto_post2017, 'manual_review_pluto17')
    print('dealt with PW only buildings')

    # dealing with PLUTO only buildings
    manually_created = pd.concat([for_mlsli, for_other, for_rls])
    pluto_only = pluto_gdf[~pluto_gdf['bbl'].isin(matched_buildings['bbl'])]
    address_match, geo_join_shown, geo_join_hidden, not_found = \
        update_pluto_only(pluto_only, manually_created, preso)
    address_match.to_csv('data/address_match.csv')
    update_df2 = update_buildings(address_match, mapped_fields)
    update_df2['new_value'] = [convert_geos(x, field) for x, field in zip(update_df2['new_value'], update_df2['field_name'])]

    pd.concat([update_df, update_df2]).to_csv('data/update_df.csv')
    geo_join_hidden.to_csv('data/final_create_buildings.csv')
    geo_join_shown.to_csv('data/final_manual_review_geo_join.csv')
    not_found.to_csv('data/final_manual_review_no_geo_join.csv') # TODO: should be create
    print('dealt with PLUTO only buildings')

    # Updating Amenities fields
    #amenities = get_pw_data('Amenities_pull_query.sql')
    with open('Amenities_pull_query.sql', 'r') as query_file:
                query_string = query_file.read()

    amenities = pull_data(query_string)
    amenities_update_df = update_amenities(matched_buildings, amenities)
    amenities_update_df.to_csv('data/final_update_amenities.csv')

if __name__ == '__main__':
    main()
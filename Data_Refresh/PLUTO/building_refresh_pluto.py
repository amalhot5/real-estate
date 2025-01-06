"""
    This script is used to update the data in Perchwell's building table
    with the latest PLUTO version
    
    Author(s): Arnav Malhotra
    Created On: 2023-06-21
    Updated On:
    Reviewer:
"""

import pandas as pd
import sqlalchemy as db
import numpy as np
import geopandas as gpd
import json
import re

from sqlalchemy import create_engine, text
from shapely.geometry import shape
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from data.library.pull_PW_data import pull_data


def update_buildings(matched_buildings, mapped_fields):
    update_df = {'id': [], 'field_name': [], 'old_value': [],
             'new_value': [], 'in_search': []}
    for fnames in mapped_fields:
        wrong = matched_buildings[matched_buildings[f'{fnames[0]}'] != \
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

# downloading PW buildings table
with open('pw_query.sql', 'r') as query_file:
    query_string = query_file.read()
    
preso = pull_data(query_string)
print('done getting PW data')

# reading the PLUTO files
pluto = pd.read_csv("data/pluto_23v1_1.csv")
shapefile_path = 'data/nyc_mappluto_23v1_1_shp/MapPLUTO.shp'
print('done getting pluto data')
gdf = gpd.read_file(shapefile_path)
pluto = pluto[pluto['bbl'].isin(gdf['BBL'].to_list())]
print('done cleaning pluto data')

# Identifying Buildings that Need Updates
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
                 # TODO: mapped fields?
                 ('landmark_x', 'landmark_y')]
preso['source_id'] = pd.to_numeric(preso['source_id'])
matched_buildings = preso.merge(pluto, how='inner', left_on='source_id',\
                                right_on='bbl')
# map mismatched fields
matched_buildings['zip'] = pd.to_numeric(matched_buildings['zip'])
matched_buildings['numfloors'] = np.ceil(matched_buildings['numfloors'])
# map landmark field
matched_buildings['landmark_y'] = [True if type(x) == str else False\
                                    for x in matched_buildings['landmark_y']]
matched_buildings['landmark_x'] = [x if x else 'f' for x in\
                                    matched_buildings['landmark_x']]
matched_buildings['landmark_x'] = \
                    [False if (x.lower() in ('f', 'false') or x.isspace()) \
                        else True for x in matched_buildings['landmark_x']]

update_df = update_buildings(matched_buildings, mapped_fields)

update_df.to_csv('data/final_buildings_update.csv')
print(f"done updating buildings")
print(f"num of unique buildings to update: f{update_df['id'].nunique()}")

# Dealing with Perchwell only buildings
in_pw = preso[~(preso['id'].isin(matched_buildings['id']))]
already_hidden = in_pw[in_pw['in_search'] != True]
in_pw = in_pw[in_pw['in_search'] == True]
keep = in_pw[~((in_pw['source'].str.contains('manual')) | \
               (in_pw['source'].str.contains('pluto')))]

in_pw = in_pw[(in_pw['source'].str.contains('manual')) | \
              (in_pw['source'].str.contains('pluto'))]
in_pw['num_rls'].fillna(0, inplace=True)
in_pw['num_mlsli'].fillna(0, inplace=True)
in_pw['num_other_listings'].fillna(0, inplace=True)
in_pw['num_acris'].fillna(0, inplace=True)
no_listings = in_pw[(in_pw['num_acris'] + \
                     in_pw['num_mlsli'] + \
                     in_pw['num_other_listings'] + \
                     in_pw['num_rls']) == 0]

has_listings = in_pw[~in_pw['id'].isin(no_listings['id'])]
from_pluto = has_listings[has_listings['source'].str.contains('pluto')]
manually_created = has_listings[has_listings['source'].str.contains('manual')]
from_pluto['last_acris_sale'] = \
    pd.to_datetime(from_pluto['last_acris_sale'],format='%Y-%m-%d',\
                    errors='coerce')
from_pluto_post2017 = from_pluto[\
    (from_pluto['last_acris_sale'] >= datetime.fromisoformat('2017-01-01')) | \
    (from_pluto['last_listing'] >= datetime.fromisoformat('2017-01-01')) | \
    (from_pluto['last_mlsli_listing'] >= datetime.fromisoformat('2017-01-01'))]
pluto_pre2017 = from_pluto[~(from_pluto['id'].isin(from_pluto_post2017['id']))]

# remove in vs outside nyc
for_mlsli = manually_created[(manually_created['num_mlsli'] > 0) &\
                             (manually_created['num_rls'] == 0)]
for_rls = manually_created[(manually_created['num_mlsli'] == 0) &\
                           (manually_created['num_rls'] > 0)]
for_other = manually_created[~(manually_created['id'].isin(for_mlsli['id'])) &\
                             ~(manually_created['id'].isin(for_rls['id']))]

manual_review = pd.concat([for_mlsli, for_other, for_rls, from_pluto_post2017])
manual_review.to_csv('data/final_manual_review.csv')
                     
dep = pd.concat([pluto_pre2017, no_listings])
dep.to_csv('data/final_deprecate.csv')
print('done with PW only buildings')

in_pluto = gdf[~gdf['BBL'].isin(matched_buildings['bbl'])]
in_pluto = in_pluto[(in_pluto['Borough'] != 'SI') & \
                    (in_pluto['Longitude'] < -73.74376451)]
resbuildings = in_pluto[in_pluto['ResArea'] > 0]

# TODO change PLUTO only section
address_dict = {
  "FIRST": "1ST",
  "SECOND": "2ND",
  "THIRD": "3RD",
  "FOURTH": "4TH",
  "FIFTH": "5TH",
  "SIXTH": "6TH",
  "SEVENTH": "7TH",
  "EIGHTH": "8TH",
  "NINTH": "9TH",
  "TENTH": "10TH",
  "ELEVENTH": "11TH",
  "TWELFTH": "12TH",
  "THIRTEENTH": "13TH",
  "FOURTEENTH": "14TH",
  "FIFTEENTH": "15TH",
  "SIXTEENTH": "16TH",
  "SEVENTEENTH": "17TH",
  "EIGHTEENTH": "18TH",
  "NINETEENTH": "19TH",
  "TWENTIETH": "20TH",
  "TWENTY-FIRST": "21ST",
  "TWENTY-SECOND": "22ND",
  "TWENTY-THIRD": "23RD",
  "TWENTY-FOURTH": "24TH",
  "TWENTY-FIFTH": "25TH",
  "TWENTY-SIXTH": "26TH",
  "TWENTY-SEVENTH": "27TH",
  "TWENTY-EIGHTH": "28TH",
  "TWENTY-NINTH": "29TH",
  "THIRTIETH": "30TH",
  "THIRTY-FIRST": "31ST",
  "THIRTY-SECOND": "32ND",
  "THIRTY-THIRD": "33RD",
  "THIRTY-FOURTH": "34TH",
  "THIRTY-FIFTH": "35TH",
  "THIRTY-SIXTH": "36TH",
  "THIRTY-SEVENTH": "37TH",
  "THIRTY-EIGHTH": "38TH",
  "THIRTY-NINTH": "39TH",
  "FORTIETH": "40TH",
  "FORTY-FIRST": "41ST",
  "FORTY-SECOND": "42ND",
  "FORTY-THIRD": "43RD",
  "FORTY-FOURTH": "44TH",
  "FORTY-FIFTH": "45TH",
  "FORTY-SIXTH": "46TH",
  "FORTY-SEVENTH": "47TH",
  "FORTY-EIGHTH": "48TH",
  "FORTY-NINTH": "49TH",
  "FIFTIETH": "50TH",
  "FIFTY-FIRST": "51ST",
  "FIFTY-SECOND": "52ND",
  "FIFTY-THIRD": "53RD",
  "FIFTY-FOURTH": "54TH",
  "FIFTY-FIFTH": "55TH",
  "FIFTY-SIXTH": "56TH",
  "FIFTY-SEVENTH": "57TH",
  "FIFTY-EIGHTH": "58TH",
  "FIFTY-NINTH": "59TH",
  "SIXTIETH": "60TH",
  "SIXTY-FIRST": "61ST",
  "SIXTY-SECOND": "62ND",
  "SIXTY-THIRD": "63RD",
"SIXTY-FOURTH": "64TH",
"SIXTY-FIFTH": "65TH",
"SIXTY-SIXTH": "66TH",
"SIXTY-SEVENTH": "67TH",
"SIXTY-EIGHTH": "68TH",
"SIXTY-NINTH": "69TH",
"SEVENTIETH": "70TH",
"SEVENTY-FIRST": "71ST",
"SEVENTY-SECOND": "72ND",
"SEVENTY-THIRD": "73RD",
"SEVENTY-FOURTH": "74TH",
"SEVENTY-FIFTH": "75TH",
"SEVENTY-SIXTH": "76TH",
"SEVENTY-SEVENTH": "77TH",
"SEVENTY-EIGHTH": "78TH",
"SEVENTY-NINTH": "79TH",
"EIGHTIETH": "80TH",
"EIGHTY-FIRST": "81ST",
"EIGHTY-SECOND": "82ND",
"EIGHTY-THIRD": "83RD",
"EIGHTY-FOURTH": "84TH",
"EIGHTY-FIFTH": "85TH",
"EIGHTY-SIXTH": "86TH",
"EIGHTY-SEVENTH": "87TH",
"EIGHTY-EIGHTH": "88TH",
"EIGHTY-NINTH": "89TH",
"NINETIETH": "90TH",
"NINETY-FIRST": "91ST",
"NINETY-SECOND": "92ND",
"NINETY-THIRD": "93RD",
"NINETY-FOURTH": "94TH",
"NINETY-FIFTH": "95TH",
"NINETY-SIXTH": "96TH",
"NINETY-SEVENTH": "97TH",
"NINETY-EIGHTH": "98TH",
"NINETY-NINTH": "99TH",
"ONE HUNDREDTH": "100TH"
}

resbuildings['address'] = [x.upper() for x in resbuildings['address']]
manually_created['display_address'] = [x.upper() for x in manually_created['display_address']]


resbuildings['address'] = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x) for x in resbuildings['address']]
resbuildings['address'] = [re.sub('-', '', x) for x in resbuildings['address']]
manually_created['display_address'] = [re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", '', x) for x in manually_created['display_address']]

address_match = manually_created.merge(resbuildings, how='inner', left_on='display_address', right_on='address')
resbuildings_no_match = resbuildings[~resbuildings['bbl'].isin(address_match['bbl'])]


preso = preso[preso.geometry_json.notnull()]
# Transform normal df to geopandas df
preso_gdf = gpd.GeoDataFrame(preso)
# Transform geojson to shapes
preso_gdf['geometry'] = [shape(json.loads(x)) for x in preso_gdf['geometry_json']]
# Set geometry for geodf
preso_gdf = preso_gdf.set_geometry('geometry')
preso_gdf.set_crs(crs="EPSG:4326", inplace=True)

preso_gdf.drop(columns=['created_at',
                        'updated_at',
                        'new_dev_start_date',
                        'new_dev_end_date',
                        'land_lease_expiration',
                        'last_rls_listing',
                        'last_mlsli_listing',
                        'last_listing',
                        'last_acris_created'], inplace=True)

shapefile_path = 'data/nyc_mappluto_23v1_1_shp/MapPLUTO.shp'
pluto_gdf = gpd.read_file(shapefile_path)
# formatting for geospatial join
pluto_gdf.to_crs('epsg:4326', inplace=True)

resbuildings_no_match = pluto_gdf[pluto_gdf['BBL'].isin(resbuildings_no_match['bbl'])]

'''
# dup check
dup_check = pluto.groupby(['address', 'zipcode'])['bbl'].nunique()
dupes = pd.DataFrame(columns=resbuildings.columns)
for address, zipcode in dup_check[dup_check >= 2].index:
    _ = resbuildings[(resbuildings['Address'] == address) & \
                     (resbuildings['ZipCode'] == zipcode)]
    dupes = pd.concat([dupes, _])

dupes_matched = dupes[dupes['BBL'].isin(matched_buildings['bbl'])] 
dupes_matched_post17 = dupes_matched[dupes_matched['YearBuilt'] >= 2017] #new construction
dupes_matched_pre17 = dupes_matched[dupes_matched['YearBuilt'] < 2017] #update other building

dupes_unmatched = dupes[~dupes['BBL'].isin(matched_buildings['bbl'])] # manual review


no_dupes = resbuildings[~resbuildings['BBL'].isin(dupes['BBL'])]

preso_geo = preso.dropna(subset=['centroid_latitude', 'centroid_longitude'])
geometry = gpd.points_from_xy(preso_geo.centroid_longitude,\
                               preso_geo.centroid_latitude)
preso_geo = preso_geo.drop(['centroid_latitude', 'centroid_longitude'], axis=1)
preso_geo = gpd.GeoDataFrame(preso_geo, crs="EPSG:4326", geometry=geometry)

sp1 = gpd.sjoin(preso_geo, no_dupes, how = 'inner', op = 'within')
sp1_unmatched = sp1[~sp1['id'].isin(matched_buildings['id'])]
sp1['num_rls'].fillna(0, inplace=True)
sp1['num_mlsli'].fillna(0, inplace=True)
sp1['num_other_listings'].fillna(0, inplace=True)
sp1['num_acris'].fillna(0, inplace=True)

sp1_no_listings = sp1[(sp1['num_acris'] + sp1['num_mlsli'] + sp1['num_other_listings'] + sp1['num_rls']) == 0]
sp1_has_listings = sp1[~sp1['BBL'].isin(sp1_no_listings['BBL'])]

sp1_has_listings = sp1_has_listings.drop(['index_right'], axis=1)

preso = preso[preso.geometry_json.notnull()]
# Transform normal df to geopandas df
preso_gdf = gpd.GeoDataFrame(preso)
# Transform geojson to shapes
preso_gdf['geometry'] = [shape(json.loads(x)) for x in preso_gdf['geometry_json']]
# Set geometry for geodf
preso_gdf = preso_gdf.set_geometry('geometry')

preso_gdf.set_crs(crs="EPSG:4326", inplace=True)

sp2 = gpd.sjoin(preso_gdf, sp1_has_listings, how = 'inner', op = 'intersects')
sp2_grouped = sp2.groupby('BBL')['id_left'].nunique()
merged_buildings = sp2_grouped[sp2_grouped > 1]
sp2_update_buildings = sp2_grouped[sp2_grouped == 1]
sp2_grouped2 = sp2.groupby('id_left')['BBL'].nunique()
split_buildings = sp2_grouped2[sp2_grouped2 > 1]

mapped_fields = [('zip', 'ZipCode'),
                 ('year_built', 'YearBuilt'),
                 #('school_district_code', 'schooldist'),
                 ('num_stories', 'NumFloors'), 
                 ('num_units', 'UnitsRes'), 
                 ('lot_area', 'LotArea'), 
                 ('lot_front', 'LotFront'), 
                 ('lot_depth', 'LotDepth'),
                 ('building_class', 'BldgClass'),
                 ('building_front', 'BldgFront'),
                 ('building_depth', 'BldgDepth'),
                 ('building_area', 'BldgArea'),
                 ('landmark', 'Landmark')]

# map mismatched fields
sp2_update_buildings['zip'] = pd.to_numeric(sp2_update_buildings['zip'])
sp2_update_buildings['NumFloors'] = np.ceil(sp2_update_buildings['NumFloors'])

# map landmark field
sp2_update_buildings['Landmark'] = [True if type(x) == str else False for x in sp2_update_buildings['Landmark']]
sp2_update_buildings['landmark'] = [x if x else 'f' for x in sp2_update_buildings['landmark']]
sp2_update_buildings['landmark'] = [False if (x.lower() in ('f', 'false') or x.isspace()) else True for x in sp2_update_buildings['landmark']]

update_df2 = update_buildings(sp2_update_buildings, mapped_fields)

update_df = pd.concat([update_df, update_df2], ignore_index=True)
update_df.to_csv('data/final_buildings_update.csv')

not_in_pw = no_dupes[~no_dupes['BBL'].isin(sp1['BBL'])]
not_in_pw_mn = not_in_pw[not_in_pw['Borough'] == 'MN'] # manual review
not_in_pw_other = not_in_pw[not_in_pw['Borough'] != 'MN'] # new building

not_in_pw = no_dupes[~no_dupes['BBL'].isin(sp1['BBL'])]
'''
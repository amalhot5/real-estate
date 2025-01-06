# Get all data sources in
import geopandas as gpd
import re
import pandas as pd

# precisely = gpd.read_file('data/precisely_ca.geojson', driver = 'GEOJSON')
# census2 =   gpd.read_file('data/ca_places/CA_Places.shp')

precisely_all_census = gpd.read_file('data/NEIGHBORHOOD_BOUNDARIES_USA_202405_SHP/data', layer='neighborhood_census_usa')
# #this is mcd layer

precisely = gpd.read_file('data/NEIGHBORHOOD_BOUNDARIES_USA_202405_SHP/data', layer='neighborhood_objects_usa')


# precisely_all['OBJ_NAME'] = precisely_all['OBJ_NAME'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
# precisely_all['OBJ_NAME'] = precisely_all['OBJ_NAME'].str.upper()


precisely = precisely.merge(precisely_all_census[['OBJ_ID','MCD_CCD','COUNTYFIPS']], on='OBJ_ID')
# precisely_la = precisely_all[(precisely_all['OBJ_SUBTYP'] == 'Neighborhood') & (precisely_all['COUNTYFIPS'] == '06037')]
# print(precisely.head())
precisely = precisely.to_crs('EPSG:4326')

precisely_mac = precisely[precisely['OBJ_SUBTYP'] =='Macro Neighborhood']
precisely_sub =precisely[precisely['OBJ_SUBTYP'] =='Sub Neighborhood']
precisely_neigh = precisely[precisely['OBJ_SUBTYP'] =='Neighborhood']


def find_county_FIPS(county_name):
    # Load the county FIPS data
    countyfips = pd.read_csv('data/county_fips_ca.csv')

    # Create the COUNTYFIPS column
    countyfips['COUNTYFIPS'] = (
        countyfips['STATEFP'].astype(str).str.zfill(2) + 
        countyfips['COUNTYFP'].astype(str).str.zfill(3)
    )

    # Standardize the county name input
    county_name_upper = county_name.upper()

    # Search for the county name directly
    match = countyfips[countyfips['COUNTYNAME'].str.upper() == county_name_upper]

    if not match.empty:
        print(f"***{county_name} found")
        return match['COUNTYFIPS'].values[0]

    # Try appending " COUNTY" to the county name and search again
    county_name_upper += ' COUNTY'
    match = countyfips[countyfips['COUNTYNAME'].str.upper() == county_name_upper]

    if not match.empty:
        print(f"***{county_name} County found")
        return match['COUNTYFIPS'].values[0]

    print(f"{county_name} does not exist in the census dataset, please retry")
    return None
def transform_name_pid(row):
    name = row["Display Name"].lower().replace(" ", "_")
    name = name.lower().replace("-", "_")
    pid = str(row["PID"])
    return f"{name}_{pid}"

def check_layer3_overlaps(layer3):
    overlaps = layer3.sjoin(layer3, predicate = 'overlaps')
    
    if len(overlaps) > 0:
        print('overlapping polys found, need revision:\n')
        return overlaps
    else:
        print('no overlaps, file ready')

def generate_layer_3(precisely_neigh, county_fips):
    mcd = gpd.read_file('data/NEIGHBORHOOD_BOUNDARIES_USA_202405_SHP/cb_2023_06_cousub_500k/cb_2023_06_cousub_500k.shp')
    #Create parent IDs
    parent_id_mapping = mcd[(mcd['STATEFP'] == county_fips[:2]) & mcd['COUNTYFP'] == county_fips[2:]]
    parent_id_mapping = mcd[['NAME','COUSUBFP']]

    precisely_filt = precisely_neigh[precisely_neigh['COUNTYFIPS'] == county_fips]

    precisely_filt = precisely_filt.merge(parent_id_mapping , how = 'left', left_on = 'MCD_CCD', right_on = 'NAME')

    layer3 = gpd.GeoDataFrame(columns = ['Display Name', 'Key', 'Selectable', 
                                                'PID', 'LAYER_3_PARENT_PID',
                                                'geometry', 'Layer Name']) 
                                                
    layer3[['Display Name', 'PID','LAYER_3_PARENT_PID', 'geometry']]  =  precisely_filt[['OBJ_NAME','OBJ_ID','COUSUBFP','geometry']]
    # layer3 = precisely_neigh[precisely_neigh['COUNTYFIPS'] == county_fips]
    layer3['Layer Name'] = 3
    
    layer3["Key"] = layer3.apply(transform_name_pid, axis=1)

    
    return (layer3)


def demote_promote_reviews(county_fips):
    precisely_mac_filt = precisely_mac[precisely_mac['COUNTYFIPS'] == county_fips]
    precisely_sub_filt = precisely_sub[precisely_sub['COUNTYFIPS'] == county_fips]

    pd.concat(
        [precisely_mac_filt,precisely_sub_filt]
            )[['OBJ_NAME',	
                    'OBJ_SUBTYP',
                    'METRO', 
                    'MCD_CCD'
                    ]].to_csv('data/out/demote_promote_review.csv', index = False) 


if __name__ == "__main__":
    county_name = 'Ventura'
    fips_code = find_county_FIPS(county_name)
    if fips_code:
        layer3 = generate_layer_3(precisely_neigh, fips_code)
        layer3.to_file(f'data/out/{county_name}_layer3.geojson')
        demote_promote_reviews(fips_code)
        print('*** Layer 3 generated')
        print(check_layer3_overlaps(layer3)[['Display Name_left','Display Name_right']])
    else:
        print(f"FIPS code for {county_name} could not be found.")
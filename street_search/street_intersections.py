# the data needed to run this script can be found here:
# https://drive.google.com/file/d/1VayQEMG6d9jwonLbfLJyBPdwCiD2fw6l/view?usp=sharing
# download and save the data/ folder in the same folder as these scripts
# Note: this contains a mapping for zip codes to boroughs and the NYC streets data pulled in Q2-2024
import pandas as pd
import geopandas as gpd
import shapely
import folium
import re
import json
from natural.number import ordinal
from tqdm.auto import tqdm
import warnings
warnings.simplefilter('ignore')

def load_shape(x):
    try:
        return shapely.wkt.loads(x)
    except:
        return shapely.geometry.shape(json.loads(x))

def load_geos(df: pd.DataFrame) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame(df[(~pd.isna(df.geometry))])
    gdf['geometry'] = [load_shape(x) for x in tqdm(gdf['geometry'], total=len(gdf))]    
    gdf = gdf.set_geometry('geometry')
    gdf.set_crs(crs="EPSG:4326", inplace=True)
    return gdf

def add_borough(s_segments: pd.DataFrame) -> pd.DataFrame:
    print('adding borough')
    zips_x_borough = pd.read_csv('data/nyc_borough_by_zip.csv')
    zips_x_borough.drop_duplicates(inplace=True)
    s_segments['ZIP'] = s_segments['L_ZIP'].combine_first(s_segments['R_ZIP'])
    correct_zip = s_segments[(pd.isna(s_segments['ZIP'])) & (s_segments['ST_NAME'] != 'CONNECTOR')]\
        .to_crs(crs="3857").sjoin_nearest(s_segments[(~pd.isna(s_segments['ZIP'])) & (s_segments['ST_NAME'] != 'CONNECTOR')]\
            .to_crs(crs="3857"))[['ZIP_right', 'PHYSICALID_left']].drop_duplicates(subset='PHYSICALID_left')

    print(len(s_segments))
    ret_df = s_segments.merge(correct_zip, how='left', left_index=True, right_index=True)
    ret_df['ZIP'] = ret_df['ZIP'].combine_first(ret_df['ZIP_right'])
    ret_df.dropna(subset='ZIP', inplace=True)
    ret_df['ZIP'] = ret_df['ZIP'].astype(int)
    ret_df = ret_df.merge(zips_x_borough, how='inner', left_on='ZIP', right_on='zip_code')
    print(len(ret_df))
    return ret_df

def load_streets(with_borough=False):
    streets = gpd.read_file('data/Centerline_20240520')
    streets.to_crs(crs="EPSG:4326", inplace=True)
    # remove irreveleant street segments
    streets = streets[~((streets['ST_LABEL'].isin(['ALLEY', 'DRIVEWAY', 'CONNECTOR', 'UNNAMED ST', 'WHITESTONE EXPRESSWAY NB EN', 'THROGS NECK EXPRESSWAY SB ET', 'United Nations Avenue N', 'United Nations Avenue S'])) |
                        (streets['ST_LABEL'].str.contains('FERRY RTE')) |
                        (streets['ST_LABEL'].str.contains('FERRY MAINTENANCE')) |
                        (streets['ST_LABEL'].str.contains('FERRY ROUTE')) |
                        (streets['ST_LABEL'].str.contains('BIKE PTH')) |
                        (streets['ST_LABEL'].str.contains('BIKE PATH')) |
                        (streets['ST_LABEL'].str.contains('BIKE CONN')) |
                        (streets['ST_LABEL'].str.contains('TUNL')) |
                        (streets['ST_LABEL'].str.contains(' STEPS')) |
                        (streets['ST_LABEL'].str.contains('BELT PARKWAY')) |
                        (streets['ST_LABEL'].str.contains('HUTCHINSON RIVER')) |
                        (streets['ST_LABEL'].str.contains('HUTCHINSON RVR')) |
                        (streets['ST_LABEL'].str.contains('DRIVEWY')) |
                        (streets['ST_LABEL'].str.contains('PEDESTIRAN')) | 
                        (streets['ST_LABEL'].str.contains('PORT RICHMOND WATER PLANT')) |
                        #(streets['ST_LABEL'].str.contains('HWY')) |
                        #(streets['ST_LABEL'].str.contains('EXPY')) |
                        (streets['ST_LABEL'].str.contains(' ET ')) | # exit abbreviation
                        (streets['ST_LABEL'].str.contains(' EN ')) | # entrance abbreviation
                        (streets['ST_LABEL'].str.contains('NB EN')) |
                        (streets['ST_LABEL'].str.contains('SB EN')) |
                        (streets['ST_LABEL'].str.contains('NB ET')) |
                        (streets['ST_LABEL'].str.contains('SB ET')) |
                        (streets['ST_LABEL'].str.contains('PARK EN')) |
                        (streets['ST_LABEL'].str.contains('PARK ET')) |
                        (streets['ST_LABEL'].str.contains('EXIT')) |
                        (streets['ST_LABEL'].str.contains('RAMP')) |
                        (streets['ST_LABEL'].str.contains('ENTRANCE')) |
                        (streets['ST_LABEL'].str.contains('DVWY')) |
                        (streets['ST_LABEL'].str.contains('BQE')) |
                        (streets['ST_LABEL'].str.contains('BROOKLYN QUEENS EXPY')) |
                        (streets['ST_LABEL'].str.contains('LONG ISLAND EXPY')) |
                        (streets['ST_LABEL'].str.contains('NASSAU EXPY')) |
                        (streets['ST_LABEL'].str.contains('CROSS BRONX EXPY')) |
                        (streets['ST_LABEL'].str.contains('GOWANUS EXPY')) |
                        (streets['ST_LABEL'].str.contains('CLEARVIEW EXPY')) |
                        (streets['ST_LABEL'].str.contains('WHITESTONE EXPY')) |
                        (streets['ST_LABEL'].str.contains('JOHN F KENNEDY EXPY')) |
                        (streets['ST_LABEL'].str.contains('MAJOR DEEGAN EXPY')) |
                        (streets['ST_LABEL'].str.contains('CROSS BRONX EXPY')) |
                        (streets['ST_LABEL'].str.contains('THROGS NECK EXPY')) |
                        (streets['ST_LABEL'].str.contains('PARK TRAIL')) |
                        (streets['ST_LABEL'].str.contains(' BRG')) |
                        (streets['ST_LABEL'].str.contains(' BRIDGE')) |
                        (streets['ST_LABEL'].str.contains('TRL')) |
                        (streets['ST_LABEL'].str.contains('PED PTH')) |
                        (streets['ST_LABEL'].str.contains('PED PATH')) |
                        (streets['ST_LABEL'].str.contains('PARK PTH')) |
                        (streets['ST_LABEL'].str.contains('BIKE AND PED PTH')) |
                        (streets['ST_LABEL'].str.contains('PARK PATH')) |
                        (streets['ST_LABEL'].str.contains('ACCESS RD')) |
                        (streets['ST_LABEL'].str.contains('ACCESS ROAD')) |
                        (streets['ST_LABEL'].str.contains(' CONNECTOR')) |
                        (streets['ST_LABEL'].str.contains('CORRECTIONAL')) |
                        (streets['ST_LABEL'].str.contains('OVERPASS')) |
                        (streets['ST_LABEL'].str.contains('UNDERPASS')) |
                        (streets['ST_LABEL'].str.contains(' OPAS')) |
                        (streets['ST_LABEL'].str.contains('GREENWAY')) |
                        #(streets['ST_LABEL'].str.contains('PARKWAY')) |
                        #(streets['ST_LABEL'].str.contains('PKWY')) |
                        (streets['ST_LABEL'].str.contains('BELT PKWY')) |
                        (streets['ST_LABEL'].str.contains('HENRY HUDSON PKWY')) |
                        (streets['ST_LABEL'].str.contains('JACKIE ROBINSON PKWY')) |
                        (streets['ST_LABEL'].str.contains('WWTP')) |
                        (streets['ST_LABEL'].str.contains('FOOTBRIDGE')) |
                        (streets['ST_LABEL'].str.contains('TURNAROUND')) |
                        (streets['ST_LABEL'].str.contains(' PED ')) |
                        (streets['ST_LABEL'].str.contains('PEDESTRIAN'))
                        )]
    streets.loc[(streets['ST_LABEL'] == 'PARK AVE VIADUCT') | (streets['ST_LABEL'] == 'PARK AVE S'), 'ST_LABEL'] = 'PARK AVE'
    streets.loc[(streets['ST_LABEL'].isin(['WASHINGTON SQ E', 'WASHINGTON SQ W', 'WASHINGTON SQ N', 'WASHINGTON SQ S'])), 'ST_LABEL'] = 'WASHINGTON SQ'
    streets.loc[(streets['ST_LABEL'].isin(['STADIUM PL N', 'STADIUM PL S'])), 'ST_LABEL'] = 'STADIUM PL'
    streets.loc[(streets['ST_LABEL'].isin(['AVE OF THE STATES N', 'AVE OF THE STATES S'])), 'ST_LABEL'] = 'AVE OF THE STATES'
    streets.loc[(streets['ST_LABEL'] == 'PORT AUTHORITY BUS TERMINAL EN'), 'ST_LABEL'] = 'PORT AUTHORITY BUS TERMINAL ENTRANCE'
    streets.loc[(streets['ST_LABEL'] == 'E MOUNT EDEN AVENUE'), 'ST_LABEL'] = 'EAST MOUNT EDEN AVENUE'
    if with_borough:
        return add_borough(streets)
    else:
        return streets
    
def address_to_display_address(s: str) -> str:
    if ' BLVD' in s:
        s = s.replace(' BLVD', ' BOULEVARD')
    if (' PL' in s) and (not ' PLACE' in s) and (not ' PLAINS' in s) and (not ((' PLZ' in s) or ('PLAZA' in s))):
        s = s.replace(' PL', ' PLACE')
    if (' ST' in s) and (not ' STREET' in s) and (not 'STATION' in s) and (not 'STATES' in s) and (not ' ST ' in s) and (not 'STROUD' in s):
        s = s.replace(' ST', ' STREET')
    if (' ST E' in s) or (' ST W' in s) or (' ST N' in s) or (' ST S' in s):
        s = s.replace(' ST ', ' STREET ')
    if ' PKWY' in s:
        s = s.replace(' PKWY', ' PARKWAY')
    if ((' AVE' in s) or ('AVE ' in s)) and ('AVERY' not in s) and (not 'AVENUE' in s) and (not 'RAVENHURST' in s):
        s = s.replace('AVE', 'AVENUE')
    if s == 'AVERY AVE':
        s = 'AVERY AVENUE'
    if s == 'RAVENHURST AVE':
        s = 'RAVENHURST AVENUE'
    if ' LN' in s:
        s = s.replace(' LN', ' LANE')
    if ' ALY' in s:
        s = s.replace(' ALY', ' ALLEY')
    if (' DR' in s) and (not 'DRIVE' in s):
        s = s.replace(' DR', ' DRIVE')
    if (' SQ' in s) and (not 'SQUARE' in s):
        s = s.replace(' SQ', ' SQUARE')
    if ' PLZ' in s:
        s = s.replace(' PLZ', ' PLAZA')
    if ' RD' in s:
        s = s.replace(' RD', ' ROAD')
    if ' EXPY' in s:
        s = s.replace(' EXPY', ' EXPRESSWAY')
    if ' TPKE' in s:
        s = s.replace(' TPKE', ' TURNPIKE')
    if (' CIR' in s) and (not 'CIRCLE' in s):
        s = s.replace(' CIR', ' CIRCLE')
    if ' CTR' in s:
        s = s.replace(' CTR', 'CENTER')
    if ' CT' in s:
        s = s.replace(' CT', ' COURT')
    if (' PROM' in s) and (not 'PROMENADE' in s):
        s = s.replace(' PROM', ' PROMENADE')
    if (' CRES' in s) and (not 'CREST' not in s):
        s = s.replace(' CRES', ' CRESENT')
    if ' FWY' in s:
        s = s.replace(' FWY', ' FREEWAY')
    if (' TER' in s) and (not 'TERMINAL' in s):
        s = s.replace(' TER', ' TERRACE')
    if ' LK' in s:
        s = s.replace(' LK', ' LAKE')
    if ' OPAS' in s:
        s = s.replace(' OPAS', ' OVERPASS')
    if ' GRN' in s:
        s = s.replace(' GRN', ' GREEN')
    if ' WALK' in s:
        s = s.replace(' WALK', ' WALKWAY')
    if ' HTS' in s:
        s = s.replace(' HTS', ' HEIGHTS')
    if ' VLG' in s:
        s = s.replace(' VLG', ' VILLAGE')
    if ' TRWY' in s:
        s = s.replace(' TRWY', ' THROUGHWAY')
    if ' HWY' in s:
        s = s.replace(' HWY', ' HIGHWAY')
    if ' HL' in s:
        s = s.replace(' HL', ' HILL')
    if ' GLN' in s:
        s = s.replace(' GLN', ' GLEN')
    if (' ESPL' in s) and (not 'ESPLANADE' in s):
        s = s.replace(' ESPL', ' ESPLANDE')
    if ' XING' in s:
        s = s.replace(' XING', ' CROSSING')
    if ' VIA' in s and not (' VIADUCT' in s):
        s = s.replace(' VIA', ' VIADUCT')
    if ' CP' in s:
        s = s.replace(' CP', ' CAMP')
    if ' CV' in s:
        s = s.replace(' CV', ' COVE')
    if (' ET' in s) and (not 'ESTATE' in s):
        s = s.replace(' ET', ' ESTATE')
    if ' CRSE' in s:
        s = s.replace(' CRSE', ' COURSE')
    if ' CMNS' in s:
        s = s.replace(' CMNS', ' COMMONS')
    if ' RDG' in s:
        s = s.replace(' RDG', ' RIDGE')
    if ' GDNS' in s:
        s = s.replace(' GDNS', ' GARDENS')
    if ' MNR' in s:
        s = s.replace(' MNR', ' MANOR')
    if ' ANX' in s:
        s = s.replace(' ANX', ' ANNEX')
    if ' EST' in s:
        s = s.replace(' EST', ' ESTATE')
    if ' BDWK' in s:
        s = s.replace(' BDWK', ' BOARDWALK')
    if ' RDWY' in s:
        s = s.replace(' RDWY', ' ROADWAY')
    if ' DRWY' in s:
        s = s.replace(' DRWY', ' DRIVEWAY')
    if ' STWY' in s:
        s = s.replace(' STWY', ' SKYWAY')
    if ' PT' in s:
        s = s.replace(' PT', ' POINT')
    if ('BCH ' in s) or (' BCH' in s):
        s = s.replace('BCH', 'BEACH')
    s = s.title()
    s = s.replace('Fdr', 'FDR')
    if (('E ' in s)) and (not 'Avenue ' in s):
        s = s.replace('E ', 'East ')
    if s[-2:] == ' E' and (not 'Avenue E' == s):
        s = s[:-2] + ' East'
    if (('W ' in s) or s[-2:] == ' W') and (not 'Avenue W' == s):
        s = s.replace('W', 'West')
    if (('N ' in s) or s[-2:] == ' N') and (not 'Avenue N' == s):
        s = s.replace('N', 'North')
    if (('S ' in s)) and (not 'Avenue S' == s) and (not 'Thomas S Boyland' in s):
        s = s.replace('S ', 'South ')
    if (s[-2:] == ' S') and (not 'Avenue S' == s) and (not 'Thomas S Boyland' in s):
        s = s.replace(' S', ' South')

    try:
        num = re.search(r'\d+', s).group(0)
        s = s.replace(num, ordinal(num))
    except:
        pass

    return s

def get_intersection_points(s1_name, s1_borough, s2_name, s2_borough, display_streets):
    if s1_name == s2_name and s1_borough == s2_borough:
        return None
    try:
        s1 = streets[(display_streets['ST_LABEL'] == s1_name) & (display_streets['borough'] == s1_borough)].reset_index()['geometry'][0]
        s2 = streets[(display_streets['ST_LABEL'] == s2_name) & (display_streets['borough'] == s2_borough)].reset_index()['geometry'][0]
        return s1.intersection(s2)
    except:
        #no_intersect += 1
        return None
    
def get_intersection_geos(intersection_point: shapely.Point):
    inter_points = shapely.buffer(intersection_point, 10/364567.2, cap_style='square')
    intersect = street_segments[street_segments.geometry.intersects(inter_points)]
    new_id = '_'.join(intersect['PHYSICALID'].astype(str))
    return new_id, shapely.ops.linemerge(shapely.geometry.MultiLineString(intersect['geometry'].values))

s_segments = load_streets(with_borough=True)
streets = pd.DataFrame(s_segments.groupby(['ST_LABEL', 'borough'])['PHYSICALID'].aggregate(lambda x: set(x)))
streets.columns = ['segment_ids']

geos = []
for index, row in streets.iterrows():
    geos.append(shapely.ops.linemerge(shapely.geometry.MultiLineString(s_segments[s_segments['PHYSICALID'].isin(row['segment_ids'])]['geometry'].values)))
streets['geometry'] = geos
streets = gpd.GeoDataFrame(streets,crs="EPSG:4326")

streets = streets.reset_index()
streets['display_label'] = [address_to_display_address(x) for x in streets['ST_LABEL']]
streets.to_csv('data/display_streets.csv')

m = folium.Map(location=[40.70, -73.94], zoom_start=10, tiles="CartoDB positron")
buildings_viz = folium.GeoJson(streets[['geometry', 'display_label', 'borough']].reset_index(), 
                               highlight_function=lambda x: {"color": 'red'}, zoom_on_click=True,
                               tooltip=folium.GeoJsonTooltip(fields=['display_label','borough']))
buildings_viz.add_to(m)
m.keep_in_front(buildings_viz)

m.save('display_streets.html')

intersections = streets.sjoin(streets, predicate='intersects')
intersections = intersections[['ST_LABEL_left', 'borough_left', 'ST_LABEL_right', 'borough_right']]
intersections.columns = ['street1_name', 'street1_borough', 'street2_name', 'street2_borough']

street_segments = load_streets(True)[['PHYSICALID', 'ST_LABEL', 'borough', 'geometry']]
intersect_geos = [get_intersection_points(a, b, c, d, streets) for a, b, c, d in tqdm(zip(intersections['street1_name'], 
                                                                                 intersections['street1_borough'], 
                                                                                 intersections['street2_name'], 
                                                                                 intersections['street2_borough']), total=len(intersections))]
intersections['geometry'] = intersect_geos

intersections = intersections[(~pd.isna(intersections['geometry'])) & (intersections['street1_name'] != intersections['street2_name'])]
int_ids = []
int_geos = []
for g in tqdm(intersections['geometry'], total=len(intersections)):
    x, y = get_intersection_geos(g)
    int_ids.append(x)
    int_geos.append(y)

intersects_with_geos = {'intersection_id': int_ids, 'geometry': int_geos}
intersects_gdf = gpd.GeoDataFrame(intersects_with_geos)
intersects_gdf = intersects_gdf.set_geometry('geometry')
intersects_gdf.set_crs(crs="EPSG:4326", inplace=True)
intersects_gdf.drop_duplicates('geometry', inplace=True)

streets_x_intersections = streets.sjoin(intersects_gdf, predicate='intersects')

streets_x_intersections[['ST_LABEL', 'borough', 'intersection_id']].to_csv('data/streets_x_intersections.csv', index=False)
intersects_gdf.to_csv('data/intersections_geos.csv', index=False)

m = folium.Map(location=[40.70, -73.94], zoom_start=10, tiles="CartoDB positron")
buildings_viz = folium.GeoJson(intersects_gdf[['geometry', 'intersection_id']].sample(10000).reset_index(), style_function=lambda x: {'opacity':0.75},
                               highlight_function=lambda x: {"color": 'red', 'opacity': 0.9}, zoom_on_click=True,
                               tooltip=folium.GeoJsonTooltip(fields=['intersection_id']))
buildings_viz.add_to(m)
m.keep_in_front(buildings_viz)
m.save('intersection_sample.html')
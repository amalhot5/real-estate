import pandas as pd
from ast import literal_eval

def get_data(units_fname, p_buildings_fname):
    try:
        units = pd.read_csv(units_fname)
        p_buildings = pd.read_csv(p_buildings_fname)
    except (FileNotFoundError, ValueError):
        # crmls_units
        cr_units_raw = \
            pd.read_csv("data/crmls_units.csv",dtype=str,escapechar="\\")
        cr_units_raw['Assessments'] = cr_units_raw['Assessments'].apply(literal_eval)
        tempdf = pd.DataFrame(cr_units_raw['Assessments'].values.tolist(),index=cr_units_raw.index, columns=["test"])
        tempdf2 = tempdf['test'].apply(pd.Series)
        units = pd.concat([cr_units_raw, tempdf2],axis=1) #, ignore_index=True
        cr_pot_unit_blds_raw = \
            pd.read_csv("data/crmls_potential_unit_buildings.csv",escapechar="\\")
        cr_pot_unit_blds_raw['Assessments'] = cr_pot_unit_blds_raw['Assessments'].apply(literal_eval)
        tempdf = pd.DataFrame(cr_pot_unit_blds_raw['Assessments'].values.tolist(),index=cr_pot_unit_blds_raw.index, columns=["test"])
        tempdf2 = tempdf['test'].apply(pd.Series)
        p_buildings = pd.concat([cr_pot_unit_blds_raw, tempdf2],axis=1)
    return units, p_buildings

def get_high_con_buildings(units, p_buildings):
    units_agg = units.groupby('ParcelFullAddress')['ogr_fid'].nunique().reset_index()
    units_agg.columns = ['ParcelFullAddress', 'unit_count']
    building_complex = p_buildings.merge(right=units_agg, right_on=['ParcelFullAddress', 'unit_count'], left_on=['ParcelFullAddress', 'NumberofUnits'])
    bcom_stats = building_complex.groupby('ParcelFullAddress')['ogr_fid'].nunique()
    high_con = building_complex[building_complex['ParcelFullAddress'].isin(bcom_stats[bcom_stats == 1].index)]
    high_con['generated_building'] = [False] * len(high_con['ParcelFullAddress'])
    return high_con

def generate_buildings(units, high_con):
    units_low_con = units[~units['ParcelFullAddress'].isin(high_con['ParcelFullAddress'])]
    new_complex = units_low_con.groupby('ParcelFullAddress')['ogr_fid'].nunique()
    buildings = pd.DataFrame({'ParcelFullAddress': new_complex.index, 'NumberofUnits': new_complex.values, 'generated_building': [True] * len(new_complex)})
    return buildings

def clean_address(buildings, apn_lookup):
    buildings['address'] = [x.rsplit(' ', 2)[0] for x in buildings['ParcelFullAddress']]
    apn_lookup['cleaned_address'] = apn_lookup['address'] + apn_lookup['city']
    buildings['address'] = ["".join(x.split()) for x in buildings['address']]
    apn_lookup['cleaned_address'] = ["".join(x.split()) for x in apn_lookup['cleaned_address']]
    return buildings, apn_lookup

def get_parcelapn(buildings_df, apn_lookup_fname):
    address_x_apn = pd.read_csv(apn_lookup_fname)
    buildings, address_x_apn = clean_address(buildings_df, address_x_apn)
    final_df =  buildings.merge(right=address_x_apn, how='left', right_on='cleaned_address', left_on='address')
    keep_fields = ['ParcelFullAddress', 'NumberofUnits', 'parcel_apn', 'generated_building']
    final_df = final_df[keep_fields]
    final_df.columns = ['ParcelFullAddress', 'NumberofUnits', 'parcelapn', 'generated_building']
    print(final_df.head())
    return final_df


def main():
    units, p_buildings = get_data('data/clean_crmls_units.csv', 'data/clean_crmls_potential_unit_buildings.csv')
    high_con = get_high_con_buildings(units, p_buildings)
    buildings = generate_buildings(units, high_con)
    buildings = get_parcelapn(buildings, 'data/crmls_addressCity_x_parcelapn.csv')
    needed_fields = ['ParcelFullAddress', 'parcelapn', 'taxapn', 
                     'TotalAssessedValue', 'TaxAmount', 'BuildingArea', 
                     'NumberOfBuildings', 'NumberofUnits', 'YearBuilt', 
                     'NumberOfStories','TotalNumberofRooms', 'NumberofBedrooms',
                     'NumberofBaths', 'AssessedLandValue',
                     'AssessedImprovementValue', 'generated_building']
    high_con = high_con[needed_fields]
    pd.concat([high_con, buildings]).to_csv('data/buildings.csv', index=False)

if __name__ == '__main__':
    main()
import pandas as pd
import pickle as pkl
import os

from datetime import timedelta

DAY_THRESH = (timedelta(days=0), None)
PRICE_THRESH = (0, None)

base_fp = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LX_HISTORICALS_DATA_FOLDER = base_fp + '/data/SF_HistoricalTransaction_IA_20240131'
LX_DATA_FOLDER = base_fp + '/data/SF_Professional_IA_20240115'
PW_FPATH = base_fp + '/data/ICAAR_Babylon_Closings_2024_03_19 (1).csv'

def load_lightbox_data(fpath: str, service_zips: list) -> pd.DataFrame:
    df = pd.read_csv(fpath)
    df = df[['DEED_LID','ASSESSMENT_LID', 'DATE_TRANSFER',
             'SITE_ADDR','SITE_HOUSE_NUMBER','SITE_DIRECTION','SITE_STREET_NAME',
             'SITE_MODE','SITE_UNIT_NUMBER','SITE_CITY','SITE_STATE','SITE_ZIP',
             'SITE_PLUS_4','DATE_FILING','VAL_TRANSFER','LONGITUDE','LATITUDE']]
    df = df[df['SITE_ZIP'].isin(service_zips)]
    df.dropna(subset='SITE_ADDR', inplace=True)
    df.dropna(subset='VAL_TRANSFER', inplace=True)
    return df[~(df['SITE_ADDR'] == 'N A')]

def load_pw_data(s: str) -> pd.DataFrame:
    if '.csv' in s:
        return pd.read_csv(s)
    
def parcel_matching(lb_closings: pd.DataFrame, pw_closings: pd.DataFrame, day_threshold: tuple, price_threshold: tuple) -> pd.DataFrame:
    parcels_x_assessments = pd.read_csv(f'{LX_DATA_FOLDER}/ParcelAssessmentRelation_IA.csv')
    parcels_x_assessments.columns = ['FIPS_CODE','ASSESSMENT_LID','PARCEL_LID'.lower()]
    listings_with_oms = lb_closings.merge(parcels_x_assessments, how='inner', on='ASSESSMENT_LID').merge(pw_closings, how='inner', on='parcel_lid')
    #print(f"matches before thresholds: {listings_with_oms['id'].nunique()}")
    if price_threshold[1]:
        if day_threshold[1]:
            return listings_with_oms[(
                abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] < price_threshold[1])
                & (abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] >= price_threshold[0])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) < day_threshold[1])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) >= day_threshold[0])][['id', 'DEED_LID']]
        else:
            return listings_with_oms[(
                abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] < price_threshold[1])
                & (abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] >= price_threshold[0])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) >= day_threshold[0])][['id', 'DEED_LID']]
    else:
        if day_threshold[1]:
            return listings_with_oms[
                (abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] >= price_threshold[0])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) < day_threshold[1])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) >= day_threshold[0])][['id', 'DEED_LID']]
        else:
            return listings_with_oms[
                (abs(listings_with_oms['VAL_TRANSFER'] - listings_with_oms['sold_price'])/listings_with_oms['VAL_TRANSFER'] >= price_threshold[0])
                & (abs(listings_with_oms['DATE_TRANSFER'] - listings_with_oms['last_event']) >= day_threshold[0])][['id', 'DEED_LID']]

def main(day_range: tuple, price_range: tuple):
    lb_fpath = f'{LX_HISTORICALS_DATA_FOLDER}/Deed_IA.csv'
    service_zips = [50104,50106,50112,50136,50153,50157,50171,50207,50242,50255,50268,50606,50607,50612,50613,50626,50629,
                    50634,50641,50643,50644,50647,50648,50650,50651,50654,50662,50667,50669,50671,50675,50682,50701,50702,
                    50703,50707,52032,52033,52035,52038,52040,52041,52042,52043,52044,52047,52048,52049,52050,52052,52053,
                    52057,52065,52066,52072,52073,52076,52077,52078,52141,52156,52157,52158,52159,52162,52201,52202,52203,
                    52205,52206,52208,52209,52210,52211,52212,52213,52214,52215,52216,52218,52219,52220,52221,52222,52223,
                    52224,52225,52227,52228,52229,52231,52232,52233,52235,52236,52237,52240,52241,52242,52245,52246,52247,
                    52248,52249,52251,52253,52255,52257,52301,52302,52305,52306,52307,52308,52309,52310,52312,52313,52314,
                    52315,52316,52317,52318,52320,52321,52322,52323,52324,52325,52326,52327,52328,52329,52330,52332,52333,
                    52334,52335,52336,52337,52338,52339,52340,52341,52345,52346,52347,52349,52351,52352,52353,52354,52355,
                    52356,52358,52359,52361,52362,52401,52402,52403,52404,52405,52411,52501,52530,52533,52535,52536,52537,
                    52540,52548,52550,52552,52553,52554,52556,52557,52561,52563,52566,52567,52576,52580,52585,52591,52619,
                    52621,52623,52624,52625,52626,52627,52630,52632,52635,52639,52640,52641,52644,52645,52646,52647,52649,
                    52651,52653,52654,52656,52657,52658,52659,52720,52721,52722,52726,52728,52737,52738,52739,52745,52746,
                    52747,52748,52749,52752,52753,52754,52755,52756,52758,52760,52761,52765,52766,52767,52768,52769,52772,
                    52773,52776,52777,52778,52801,52802,52803,52804,52806,52807]

    '''try:
        with open(f'{base_fp}/data/off_market_sales.pkl', 'rb') as f:
            deeds_df = pkl.load(f)
    except:
    try:
        with open(f'{LX_HISTORICALS_DATA_FOLDER}/deeds_df.pkl', 'rb') as f:
            deeds_df = pkl.load(f)
    except:'''
        # load lightbox data
    deeds_df = load_lightbox_data(lb_fpath, service_zips)
        #with open(f'{LX_HISTORICALS_DATA_FOLDER}/deeds_df.pkl', 'wb') as f:
         #   pkl.dump(deeds_df, f)
    #print('done loading deeds')

    # load perchwell data (from reso_closed_listings.sql)
    '''try:
        with open(base_fp + '/data/unmatched_closings.pkl', 'rb') as f:
            pw_closings = pkl.load(f)
    except:'''
    pw_closings = load_pw_data(PW_FPATH)
    pw_closings['address'] = pw_closings['address'].str.replace('.', '')
    pw_closings['address'] = pw_closings['address'].str.strip()
    pw_closings['last_event'] = pd.to_datetime(pw_closings['last_event'], dayfirst=True)
    
    deeds_df['DATE_TRANSFER'] = pd.to_datetime(deeds_df['DATE_TRANSFER'])
    #print('done loading pw data')

    parcel_match = parcel_matching(deeds_df, pw_closings, day_range, price_range)
    
    print(f"done parcel match: {len(pw_closings[pw_closings['id'].isin(parcel_match['id'])])} out of {len(pw_closings)}")
    print(f"{len(pw_closings[pw_closings['id'].isin(parcel_match['id'])])/len(pw_closings) * 100}%")

    with open(f'{base_fp}/data/parcel_matching.pkl', 'wb') as f:
        pkl.dump(parcel_match, f)
    with open(base_fp + '/data/unmatched_closings.pkl', 'wb') as f:
        pkl.dump(pw_closings[~pw_closings['id'].isin(parcel_match['id'])], f)
    with open(base_fp + '/data/off_market_sales.pkl', 'wb') as f:
        pkl.dump(deeds_df[~deeds_df['DEED_LID'].isin(parcel_match['DEED_LID'])], f)

    return parcel_match

if __name__ == '__main__':
    main(DAY_THRESH, PRICE_THRESH)
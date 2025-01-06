import pandas as pd

from datetime import timedelta
from tqdm import tqdm

DAY_THRESH = timedelta(days=90)
PRICE_THRESH = 0.3

LIGHTBOX_DATA_FOLDER = 'data/SF_HistoricalTransaction_IA_20240131'
PW_FPATH = 'data/Reso_Closed_Listings_2024_03_12.csv'

def load_lightbox_data(fpath: str, service_zips: list) -> pd.DataFrame:
    df = pd.read_csv(fpath)
    df = df[['DEED_LID', 'SITE_ADDR', 'SITE_CITY', 'SITE_ZIP', 'DATE_TRANSFER', 'VAL_TRANSFER', 'SITE_HOUSE_NUMBER', 'SITE_DIRECTION', 'SITE_STREET_NAME', 'SITE_UNIT_NUMBER','SITE_MODE', 'ASSESSMENT_LID']]
    df = df[df['SITE_ZIP'].isin(service_zips)]
    df.dropna(subset='SITE_ADDR', inplace=True)
    df.dropna(subset='VAL_TRANSFER', inplace=True)
    return df[~(df['SITE_ADDR'] == 'N A')]

def load_pw_data(s: str) -> pd.DataFrame:
    if '.csv' in s:
        return pd.read_csv(s)
    
def standardize_address(address: str) -> str:
    address = address.upper()
    address = address.replace('(', '')
    address = address.replace(')', '')
    for index, row in suffixes.iterrows():
        if ' ' + row['Alias'] in address:
            address = address.replace(row['Alias'], row['Abbreviation'])
    return address

def fuzzy_match(lb_address: str, pw_address: str, listing_id, lb_price, pw_price, lb_date, pw_date): #, lb_zip, pw_zip):
    l = lb_address.split()
    p = pw_address.split()
    m_count = 0
    if len(l) > len(p):
        len_address = len(p)
        for i in l:
            if i in p: m_count += 1
    else:
        len_address = len(l)
        for i in p:
            if i in l: m_count += 1
    try:
        if m_count / len_address >= 0.66 and (abs((lb_price - pw_price) / pw_price) <= PRICE_THRESH) and (abs(pw_date - lb_date) <= DAY_THRESH):# and lb_zip == pw_zip:
            return listing_id
        else:
            return None
    except ZeroDivisionError:
        return None
    
def dedupe(df:pd.DataFrame) -> list:
    dupe_sales = []
    for index, row in tqdm(df.iterrows(), total=len(df)):
        if row['DEED_LID'] not in dupe_sales:
            dupes = df[(row['SITE_ADDR'] == df['SITE_ADDR']) &
                       (abs(row['DATE_TRANSFER'] - df['DATE_TRANSFER']) <= DAY_THRESH) &
                       (abs((row['VAL_TRANSFER'] - df['VAL_TRANSFER'])/row['VAL_TRANSFER']) <= PRICE_THRESH)] 
            
            dupe_sales += list(dupes[dupes['DEED_LID'] != row['DEED_LID']]['DEED_LID'].values)
    
    return dupe_sales

lb_fpath = f'{LIGHTBOX_DATA_FOLDER}/Deed_IA.csv'
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

# load lightbox data
deeds_df = load_lightbox_data(lb_fpath, service_zips)
print('done loading deeds')

# load perchwell data (from reso_closed_listings.sql)
pw_closings = load_pw_data(PW_FPATH)
pw_closings['unparsed_address'] = pw_closings['unparsed_address'].str.replace('.', '')
pw_closings['unparsed_address'] = pw_closings['unparsed_address'].str.strip()
pw_closings['close_date'] = pd.to_datetime(pw_closings['close_date'], dayfirst=True)
deeds_df['DATE_TRANSFER'] = pd.to_datetime(deeds_df['DATE_TRANSFER'])
print('done loading pw data')

# load suffix mapping
suffixes = pd.read_csv('usps street suffixes.csv')
suffixes.columns = ['Street Suffix', 'Alias', 'Abbreviation']
suffixes.fillna(method='ffill', inplace=True)
suffixes = suffixes[['Alias', 'Abbreviation']]

# matching first pass, direct address matches only
pw_closings['match_key'] = [x.upper() for x in pw_closings['unparsed_address']]
first_match = pw_closings.merge(deeds_df, how='inner', left_on='match_key', right_on='SITE_ADDR').drop('match_key', axis=1)
first_match = first_match[abs(first_match['close_date'] - first_match['DATE_TRANSFER']) <= DAY_THRESH]
first_match = first_match[abs((first_match['close_price'] - first_match['VAL_TRANSFER'])/first_match['close_price']) <= PRICE_THRESH]
print(f"done first match: {len(pw_closings[pw_closings['listing_id'].isin(first_match['listing_id'])])}")

pw_closings['normalized_address'] = [standardize_address(x) for x in tqdm(pw_closings['unparsed_address'])]

off_market_sales = deeds_df[~deeds_df['DEED_LID'].isin(first_match['DEED_LID'])]
second_match = pw_closings[~pw_closings['listing_id'].isin(first_match['listing_id'])]
second_match = second_match.merge(off_market_sales, how='inner', left_on='normalized_address', right_on='SITE_ADDR')
second_match = second_match[abs((second_match['close_price'] - second_match['VAL_TRANSFER'])/second_match['close_price']) <= PRICE_THRESH]
second_match = second_match[abs(second_match['close_date'] - second_match['DATE_TRANSFER']) <= DAY_THRESH]
print(f"done second match: {len(pw_closings[(pw_closings['listing_id'].isin(second_match['listing_id']))])}")

off_market_sales = off_market_sales[~off_market_sales['DEED_LID'].isin(second_match['DEED_LID'])]
third_match = pw_closings[~(pw_closings['listing_id'].isin(first_match['listing_id'])) & ~(pw_closings['listing_id'].isin(second_match['listing_id']))]

third_match['check_zip'] = [int(x.split('-')[0]) if '-' in x else int(x) for x in third_match['postal_code']]

retlist = []
for lb_address, lb_price, lb_date, lb_zip in tqdm(zip(off_market_sales['SITE_ADDR'], off_market_sales['VAL_TRANSFER'], off_market_sales['DATE_TRANSFER'], off_market_sales['SITE_ZIP']), total=len(off_market_sales)):
    check = third_match[third_match['check_zip'] == lb_zip]
    found = False
    for pw_address, pw_price, pw_date, l_id in zip(check['normalized_address'], check['close_price'], check['close_date'], check['listing_id']):
        result = fuzzy_match(lb_address, pw_address, l_id, lb_price, pw_price, lb_date, pw_date)
        if result:
            found = True
            retlist.append(result)
            break
    if not found:
        retlist.append(None)

off_market_sales['matched_listing_id'] = retlist
print(f"num last match: {len(off_market_sales[~pd.isna(off_market_sales['matched_listing_id'])])}")
off_market_sales[~pd.isna(off_market_sales['matched_listing_id'])].merge(third_match, how='inner', left_on='matched_listing_id', right_on='listing_id').to_csv('data/output/fuzzy_matches.csv')
off_market_sales = off_market_sales[pd.isna(off_market_sales['matched_listing_id'])]

print(f"total oms: {len(off_market_sales)}, unmatched pw listings: {len(pw_closings[~((pw_closings['listing_id'].isin(first_match['listing_id'])) | (pw_closings['listing_id'].isin(second_match['listing_id'])) | (pw_closings['listing_id'].isin(retlist)))])}")
off_market_sales.drop('matched_listing_id', axis=1, inplace=True)
dupe_oms = dedupe(off_market_sales)
off_market_sales[~(off_market_sales['DEED_LID'].isin(dupe_oms))].to_csv('data/output/off_market_sales.csv', index=False)
pw_closings[~((pw_closings['listing_id'].isin(first_match['listing_id'])) | (pw_closings['listing_id'].isin(second_match['listing_id'])) | (pw_closings['listing_id'].isin(retlist)))].to_csv('data/output/unmatched_mls_listings.csv', index=False)
print(f"total oms: {len(off_market_sales[~(off_market_sales['DEED_LID'].isin(dupe_oms))])}, unmatched pw listings: {len(pw_closings[~((pw_closings['listing_id'].isin(first_match['listing_id'])) | (pw_closings['listing_id'].isin(second_match['listing_id'])) | (pw_closings['listing_id'].isin(retlist)))])}")


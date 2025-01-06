import pandas as pd
import pickle as pkl
import os

from datetime import timedelta
from tqdm.auto import tqdm

DAY_THRESH = (timedelta(days=0), None)
PRICE_THRESH = (0, None)

base_fp = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def fuzzy_match(lb_address: str, lb_street_name: str, pw_address: str, listing_id, lb_price, pw_price, lb_date, pw_date, day_threshold, price_threshold): #, lb_zip, pw_zip):
    l = lb_address.strip().split()
    p = pw_address.strip().split()
    m_count = 0

    if not ((l[0] == p[0]) and (str(lb_street_name) in pw_address)):
        return None
    
    if len(l) >= len(p):
        len_address = len(p)
        for i in l:
            if i in p: m_count += 1
    else:
        len_address = len(l)
        for i in p:
            if i in l: m_count += 1
    try:
        if price_threshold[1]:
            if day_threshold[1]:
                if (m_count / len_address >= 0.66) \
                    and (abs((lb_price - pw_price) / pw_price) < price_threshold[1])\
                        and (abs((lb_price - pw_price) / pw_price) >= price_threshold[0])\
                            and (abs(pw_date - lb_date) < day_threshold[1])\
                                and (abs(pw_date - lb_date) >= day_threshold[0]):
                    return listing_id
                else:
                    return None
            else:
                if (m_count / len_address >= 0.66) \
                    and (abs((lb_price - pw_price) / pw_price) < price_threshold[1])\
                        and (abs((lb_price - pw_price) / pw_price) >= price_threshold[0])\
                                and (abs(pw_date - lb_date) >= day_threshold[0]):
                    return listing_id
                else:
                    return None

        else:
            if day_threshold[1]:
                if (m_count / len_address >= 0.66) \
                        and (abs((lb_price - pw_price) / pw_price) >= price_threshold[0])\
                            and (abs(pw_date - lb_date) < day_threshold[1])\
                                and (abs(pw_date - lb_date) >= day_threshold[0]):
                    return listing_id
                else:
                    return None
            else:
                if (m_count / len_address >= 0.66) \
                        and (abs((lb_price - pw_price) / pw_price) >= price_threshold[0])\
                                and (abs(pw_date - lb_date) >= day_threshold[0]):
                    return listing_id
                else:
                    return None
    except ZeroDivisionError:
        return None

def third_round_match(third_match, off_market_sales, day_threshold, price_threshold):
    third_match['check_zip'] = [int(x.split('-')[0]) if '-' in x else int(x) for x in third_match['zip']]

    retlist = []
    for lb_address, lb_price, lb_date, lb_street_name, lb_zip in tqdm(zip(off_market_sales['normalized_address'],
                                                          off_market_sales['VAL_TRANSFER'],
                                                          off_market_sales['DATE_TRANSFER'], 
                                                          off_market_sales['SITE_STREET_NAME'], 
                                                          off_market_sales['SITE_ZIP']), 
                                                          total=len(off_market_sales), leave=True):
    #for lb_address, lb_price, lb_date, lb_zip in zip(off_market_sales['normalized_address'], off_market_sales['VAL_TRANSFER'], off_market_sales['DATE_TRANSFER'], off_market_sales['SITE_ZIP']):
        #if len(retlist)%10000 == 0:
         #   #print(len(retlist), len(off_market_sales))
        check = third_match[third_match['check_zip'] == lb_zip]
        found = False
        for pw_address, pw_price, pw_date, l_id in zip(check['normalized_address'], check['sold_price'], check['last_event'], check['id']):
            result = fuzzy_match(lb_address, lb_street_name, pw_address, l_id, lb_price, pw_price, lb_date, pw_date, day_threshold, price_threshold)
            if result:
                found = True
                retlist.append(result)
                break
        if not found:
            retlist.append(None)

    off_market_sales['matched_listing_id'] = retlist
    off_market_sales[~pd.isna(off_market_sales['matched_listing_id'])].merge(third_match, how='inner', left_on='matched_listing_id', right_on='id').to_csv(f'{base_fp}/data/output/fuzzy_matches.csv')
    return off_market_sales[~pd.isna(off_market_sales['matched_listing_id'])][['matched_listing_id', 'DEED_LID']]

def main(day_range: tuple, price_range: tuple) -> None:
    #print('starting fuzzy match')
    with open(base_fp + '/data/unmatched_closings.pkl', 'rb') as f:
        pw_closings = pkl.load(f)
    with open(base_fp + '/data/off_market_sales.pkl', 'rb') as f:
        deeds_df = pkl.load(f) 

    third_match = third_round_match(pw_closings, deeds_df, day_range, price_range)
    print(f"done third match: {len(pw_closings[(pw_closings['id'].isin(third_match['matched_listing_id']))])} out of {len(pw_closings)}")
    print(f"{len(pw_closings[pw_closings['id'].isin(third_match['matched_listing_id'])])/(len(pw_closings)) * 100}%")
    #print(f"Unmatched listings from ICAAR: {len(pw_closings[~pw_closings['id'].isin(third_match['matched_listing_id'])])}")

    with open(base_fp + '/data/fuzzy_match.pkl', 'wb') as f:
        pkl.dump(third_match, f)
    with open(f'{base_fp}/data/unmatched_closings.pkl', 'wb') as f:
        pkl.dump(pw_closings[~pw_closings['id'].isin(third_match['matched_listing_id'])], f)
    pw_closings[~pw_closings['id'].isin(third_match['matched_listing_id'])].to_csv(f'{base_fp}/data/output/unmatched_mls_listings_{day_range[0].days}_{price_range[0]*100:.0f}.csv', index=False)

    with open(base_fp + '/data/off_market_sales.pkl', 'wb') as f:
        pkl.dump(deeds_df[~deeds_df['DEED_LID'].isin(third_match['DEED_LID'])], f)

    third_match.columns = ['id', 'DEED_LID']
    return third_match

if __name__ == '__main__':
    main(DAY_THRESH, PRICE_THRESH)
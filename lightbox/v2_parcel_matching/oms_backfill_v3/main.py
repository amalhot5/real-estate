import a_parcel_matching
import b_first_match
import c_second_match
import d_fuzzy_match
import e_threshold_matching

from e_dedupe_oms import dedupe

from tqdm.notebook import tqdm
from datetime import timedelta

import pandas as pd
import pickle as pkl
import os

import warnings
warnings.filterwarnings('ignore')

DAY_THRESHOLD = (timedelta(days=0), timedelta(days=180))
PRICE_THRESHOLD = (0, None)
base_fp = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def dedupe_oms(day_threshold, price_threshold):
    with open(base_fp + '/data/off_market_sales.pkl', 'rb') as f:
        deeds_df = pkl.load(f) 

    dupe_oms = dedupe(deeds_df[['DEED_LID', 'normalized_address', 'DATE_TRANSFER', 'VAL_TRANSFER', 'ASSESSMENT_LID']], day_threshold, price_threshold)
    
    with open(f'{base_fp}/data/dupe_oms.pkl', 'wb') as f:
        pkl.dump(dupe_oms, f)

    deeds_df[~(deeds_df['DEED_LID'].isin(dupe_oms))].to_csv(f'{base_fp}/data/output/off_market_sales.csv', index=False)

    print(f"total oms: {len(deeds_df[~(deeds_df['DEED_LID'].isin(dupe_oms))])}")

def main(day_range, price_range):
    parcel_match = a_parcel_matching.main(day_range, price_range)
    first_match = b_first_match.main(day_range, price_range)
    second_match = c_second_match.main(day_range, price_range)
    third_match = d_fuzzy_match.main(day_range, price_range)
    threshold_match = e_threshold_matching.main((timedelta(days=0), timedelta(days=30)), (0, 0.05))

    parcel_match['type_of_match'] = ['0_parcel_match'] * len(parcel_match)
    first_match['type_of_match'] = ['1_exact_address'] * len(first_match)
    second_match['type_of_match'] = ['2_standardized_address'] * len(second_match)
    third_match['type_of_match'] = ['3_fuzzy_match'] * len(third_match)
    threshold_match['type_of_match'] = ['4_threshold_match'] * len(threshold_match)

    
    all_matches = pd.concat([parcel_match, first_match, second_match, third_match, threshold_match])
    all_matches.to_csv(f'{base_fp}/data/output/matched_deeds_with_closings.csv', index=False)

    dedupe_oms(DAY_THRESHOLD[1], 1)

if __name__ == '__main__':
    main(DAY_THRESHOLD, PRICE_THRESHOLD)


# Closed Listings Match to Tax Closings Execution I & II 

## 3RD Round Match

import pandas as pd

import numpy as np

from datetime import datetime
import censusgeocode as cg

# REPLACE PATH TO REMAINING UNMATCHED:

remaining_unmatched = pd.read_csv("/Users/perchwellallusers/Perchwell/Tax Municipals Closings Matching/remaining_to_geocode.csv")

# GEO CODE
clgeodf = pd.DataFrame()
for reqbatch in range(0,len(togeocode),5000):
    endrang = reqbatch + 5000
    reqdf = togeocode[reqbatch:endrang][['listing_key','street_address','city','state','postal_code']] # to change listing_id to listing_key
    tocsv = reqdf.to_csv('temp.csv', index=False)
    result = cg.addressbatch('temp.csv')
    tempdf = pd.DataFrame.from_records(result)
    clgeodf = clgeodf.append(tempdf)
    print('processing ', reqbatch+5000)
    
# Read in address normalization data
resultsnor = clgeodf.copy()

resultsnor = resultsnor[resultsnor.id!='listing_id']

# Read in tax data: (From SQL query)
taxtrans = pd.read_csv("tax_trans.csv")

taxtrans.txs_date = taxtrans.txs_date.apply(pd.to_datetime)


# Read in the closed_listings file
closed_listings=pd.read_csv('/Users/perchwellallusers/Perchwell/Tax Municipals Closings Matching/source data/closed_listings.csv')
closed_listings.close_date = closed_listings.close_date.apply(pd.to_datetime)

# Clean up results df, remove bad lines
resultsnor.id = resultsnor.id.astype(int)

# Merge and select relevant columns, on id
closedlistings_nor = resultsnor.merge(closed_listings, how= "left", 
                                      left_on='id',
                                      right_on='listing_id')[['listing_key','unit_number', 'street_dir_prefix', 'parsed','close_price','close_date']]

# Split parsed data into tokenized fields
closedlistings_nor[['mls_addr_raw','mls_city','mls_state','mls_zip']] = closedlistings_nor.parsed.str.split(",", expand=True)

# Strip trailing spaces
closedlistings_nor = closedlistings_nor.applymap(lambda x: x.strip() if isinstance(x, str) else x)


# Replace empty strings with np.nan:
taxtrans = taxtrans.replace("",np.nan)



matchresult = closedlistings_nor.merge(taxtrans, how='inner', 
                                       left_on  = ['mls_addr_raw' ,'mls_city' ,'mls_state'  , 'mls_zip'],
                                       right_on = ['site_addr_raw','site_city', 'site_state', 'site_zip'])


matchresult['date_diff'] = abs(matchresult['txs_date']-matchresult['close_date']).dt.days
matchresult['price_diff'] = abs(matchresult['txs_price']-matchresult['close_price'])


# Rename columns to match deliverable
matchresult.columns = ['mls_key', 'mls_unit_no', 'mls_street_prefix', 'mls_addr', 'mls_price',
                       'mls_date','mls_addr_raw','mls_city', 'mls_state'  , 'mls_zip',
                       'txs_id', 'txs_unit_no', 'txs_unit_prefix', 'txs_street_prefix','txs_addr', 'txs_price', 'txs_date',
                       'assessment_lid', 'site_addr_raw', 'site_city', 'site_state',
                       'site_zip', 'date_diff', 'price_diff']



# Read in round1,2,3 results, omit mls_keys that are in the file:
master = pd.read_csv("/Users/perchwellallusers/Downloads/CINCY Master Matching Document - Hollie's matches.csv")

# Remove duplicate matches
matchresult = matchresult[~matchresult.mls_key.isin(master.mls_key.to_list())]

allmatch = matchresult[['txs_id', 'mls_key', 'mls_unit_no', 'txs_unit_no', 'txs_unit_prefix',
       'txs_street_prefix', 'mls_street_prefix', 'txs_addr', 'mls_addr',
       'txs_price', 'mls_price', 'txs_date', 'mls_date', 'assessment_lid',
       'date_diff', 'price_diff']]

firstappend = allmatch[(allmatch.price_diff == 0) & (allmatch.date_diff <= 10)]

tomaster = pd.DataFrame()

# Append to master first matching on exact price match, 30 day dif and no units
tomaster = tomaster.append(firstappend)
tomaster['match_method'] = 'Round 3 | Exact price match, 10 day dif | No units'
tomaster['date_matched'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

allmatch = pd.concat([allmatch,firstappend]).drop_duplicates(keep=False)


# Remove duplication choosing ones with lower price diff:
tomaster = tomaster.loc[tomaster.groupby('mls_key').price_diff.idxmin()]

# Append to master first matching on exact price match, 30 day dif and no units
tomaster = tomaster.append(allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff < 10)])
tomaster['match_method'] = tomaster['match_method'].fillna('Round 3 | Price diff within 1/3, 10 day dif | No units')
tomaster['date_matched'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

print(len(allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff < 10)]), 'additional matches')


allmatch = pd.concat([allmatch,allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff < 10)]]).drop_duplicates(keep=False)



# Append to master first matching on exact price match, 30 day dif and no units
tomaster = tomaster.append(allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff <= 20)])
tomaster['match_method'] = tomaster['match_method'].fillna('Round 3 | Price diff within 1/3, 20 day dif | No units')
tomaster['date_matched'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

print(len(allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff <= 20)]), 'additional matches')

allmatch = pd.concat([allmatch,allmatch[(allmatch.price_diff <= (allmatch.mls_price)/3) & (allmatch.date_diff <= 20)]]).drop_duplicates(keep=False)


allmatch[allmatch.date_diff<=60]

# Append to master first matching on exact price match, 30 day dif and no units
tomaster = tomaster.append(allmatch[allmatch.date_diff<=60])
tomaster['match_method'] = tomaster['match_method'].fillna('Round 3 | No price match, 60 day dif | No units')
tomaster['date_matched'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

print(len(allmatch[allmatch.date_diff<=60]), 'additional matches')

allmatch = pd.concat([allmatch,allmatch[allmatch.date_diff<=60]]).drop_duplicates(keep=False)



# Append to master first matching on exact price match, 100 day dif and no units
tomaster = tomaster.append(allmatch[(allmatch.price_diff==0) & (allmatch.date_diff<=100)])
tomaster['match_method'] = tomaster['match_method'].fillna('Round 3 | Exact price match, 100 day dif | No units')
tomaster['date_matched'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

print(len(allmatch[(allmatch.price_diff==0) & (allmatch.date_diff<=100)]), 'additional matches')

allmatch = pd.concat([allmatch,allmatch[(allmatch.price_diff==0) & (allmatch.date_diff<=100)]]).drop_duplicates(keep=False)




tomaster = tomaster[tomaster.price_diff.notnull()]

# Remove duplication choosing ones with lower price diff:
tomaster = tomaster.loc[tomaster.groupby('mls_key').price_diff.idxmin()]

tomaster.to_csv(f'{datetime.now().date()}_Round4_matchresult_tovalidate.csv',index=False)
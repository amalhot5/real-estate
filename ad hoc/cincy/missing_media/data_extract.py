
import pandas as pd
import requests
from tqdm import tqdm
base_path = '/Users/arnavmalhotra/Desktop/ad hoc'
# from requests.auth import HTTPBasicAuth
# url 
# github_username = "api_token"
token = "--j6IMjZHyN2aAf-L7S5yw"
'''items = [1756079,1768868,1775156,1775180,1775203,1775456,1775711,
1776247,1776502,1776528,1776566,1777001,1777065,1777170,1777274,
1777506,1777617,1777899,1778076,1778429,1778450,1778664,1779044,
1779077,1779103,1779149,1779165,1779239,1779373,1779431,1779517,
1779521,1779571,1779584,1779588,1779615,1779618,1779649,1779672,
1779712,1779793,1779797,1779798,1779800,1779856,1779865,1779870,
1779877,1779879,1779908,1779923,1780007,1780046,1780086,1780148,
1780161,1780246,1780252,1780280,1780290]
'''
#listings_to_check = pd.read_excel('Results.xlsx',sheet_name='Sheet1')
listings_to_check = pd.read_csv('all_cincy_published_listings_2023_11_14.csv')
items = listings_to_check['rebny_id'].unique()

errors=[]
data_lst = []
for item in tqdm(items, total=len(items)):
    try:
        response = requests.get(
        url= f"https://www.perchwell.com/api/v1/OData/cincymls/Property?$top=1&$skip=0&$filter=ListingId eq '{item}'",
        headers={'Authorization': f'Bearer {token}'})
        # if response.status_code == 200:
        resp_data = response.json()
        status = resp_data['value'][0].get("StandardStatus",'N/A')
        if status == 'Closed':
            data_lst.append(item)

    except:
        errors.append(item)
        continue


def report_writer(dfs:dict, report_name:str):
    """Creates an excel report using a dictionary of dfs.

    Args:
        dfs (dict): Data
        report_name (str): Name of file.
    """
    writer = pd.ExcelWriter(report_name, engine='xlsxwriter')
    workbook = writer.book
    format = workbook.add_format()
    format.set_align('center')
    for sheetname, df in dfs.items():
        df.to_excel(writer, sheet_name=sheetname, index=False)
        worksheet = writer.sheets[sheetname]
        for idx, col in enumerate(df):
            series =df[col]
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            ))+1
            worksheet.set_column(idx, idx, max_len, format)

    writer.close()

mydict = {'closed': pd.DataFrame({'rebny_id': data_lst}), 'errors': pd.DataFrame({'rebny_id': errors})}
report_writer(mydict, 'cincy_listings_api_closed_errors.xlsx')
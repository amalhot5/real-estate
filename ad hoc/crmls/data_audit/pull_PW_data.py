"""
This script contains a function that will pull the results of a given SQL query and 
saves them to a CSV.
In order to connect to PW's database via Teleport, run the following commands in Terminal:
tsh login --proxy=teleport.mgmt.perchwell.com:443
tsh proxy db --db-user=teleport --db-name=perchwell --tunnel postgres-prod-replica --port=2023
"""

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pickle as pkl

import time
from tqdm import tqdm


def pull_data(query_string:str, file_out=None, port_num=2023, db_name='perchwell'):
    pw_df = []
    engine = create_engine(\
        f"postgresql://teleport:@localhost:{port_num}/{db_name}")
    start = time.time()
    try:
        # Execute the query
        with engine.connect() as conn:
            result = conn.execute(text(query_string))

            # Process the query result
            for row in result:
                # Access the row data
                pw_df.append(row)
    except:
        # in case of timeout
        engine = create_engine(\
            f"postgresql://teleport:@localhost:{port_num}/{db_name}")
        with engine.connect() as conn:
            result = conn.execute(text(query_string))
            for row in result:
                pw_df.append(row)
    if len(pw_df) != 10000:
        print(f"Number of rows: {len(pw_df)}, time taken:{time.time() - start}")
    # pw_df = pd.DataFrame(pw_df)
    if file_out:
        pd.DataFrame(pw_df).to_csv(f'{file_out}.csv', index=False)
        with open(f'data/{file_out}.pkl', 'wb') as f:
                pkl.dump(pd.DataFrame(pw_df), f)
    return pw_df

if __name__ == '__main__':
    DB = 'listings'
    DB_NAME = 'perchwell'
    with open('listing_history_errors.sql', 'r') as f:
        sql_query = f.read()
    pull_data(sql_query, f'crmls_listing_history_errors', db_name=DB_NAME)
    '''for i in tqdm(range(100)):
        with open(f'gen_queries/features/features_data_{i}.sql', 'r') as f:
            sql_query = f.read()
        pull_data(sql_query, f'features_{i}')
        with open(f'gen_queries/{DB}/{DB}_data_{i}.sql', 'r') as f:
            sql_query = f.read()
        pull_data(sql_query, f'{DB}_{i}', db_name=DB_NAME)'''
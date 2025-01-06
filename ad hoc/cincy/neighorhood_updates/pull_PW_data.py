"""
This script contains a function that will pull the results of a given SQL query and 
saves them to a CSV.
In order to connect to PW's database via Teleport, run the following commands in Terminal:
tsh login --proxy=teleport.mgmt.perchwell.com:443
tsh proxy db --db-user=teleport --db-name=perchwell --tunnel postgres-prod-read-replica --port=2023
"""

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pickle as pkl

def pull_data(sql_query:str, file_out=None, port_num=2023):
    """
    For a given sql_query, this function will connect to Perchwell's database, pull
    the corresponding data, save it to a csv in the file specified (default is 
    pw.csv), and returns that data in the form of a Pandas DataFrame.

    Args:
        sql_query (str): query whose results you wish to download
        file_out (str, optional): filepath for the output file. If None, then
            no file is created. Defaults to None.
        port_num (int, optional): Port number for Teleport Connection. Defaults
            to 2023.

    Returns:
        pd.DataFrame: DataFrame containing results of sql_query 
    """    
    if sql_query[-1] == ';':
        sql_query = sql_query[:-1]
    pw_df = []
    prev_row_count = -1
    current_row = 0
    # Create the SQLAlchemy engine
    engine = create_engine(\
        f"postgresql://teleport:@localhost:{port_num}/perchwell")

    # in order to get around the 300k row limit
    while prev_row_count <= current_row:
        if current_row > 0 and current_row % 1000000 == 0:
            with open(f'tmp_{current_row}.pkl', 'wb') as f:
                pkl.dump(pw_df, f)
        current_row = len(pw_df)
        if prev_row_count == current_row:
            print(f"final row count: {current_row}")
            break
        # Define the SQL query string, but keep last line
        query_string = f"""{sql_query}
            -- NOTE: DO NOT DELETE BELOW THIS
            LIMIT 100000 OFFSET {current_row};
            """
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
                f"postgresql://teleport:@localhost:{port_num}/perchwell")
            with engine.connect() as conn:
                result = conn.execute(text(query_string))
                for row in result:
                    pw_df.append(row)
        print(f"{len(pw_df)} rows appended")
        prev_row_count = current_row
    pw_df = pd.DataFrame(pw_df)
    if file_out:
        pw_df.to_csv(file_out)
    return pw_df


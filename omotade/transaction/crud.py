# -*- coding: utf-8 -*-
"""
Created on Tue May 17 22:36:44 2022

@author: Omotade
"""

import sys
import os
sys.path.append(os.getcwd())

try:
    from extract import extract_coindata
    from config import localtb_engine
    
except ModuleNotFoundError:
    from omotade.extract import extract_coindata
    from omotade.config import localtb_engine
engine = localtb_engine

import pandas as pd
from datetime import datetime
import logging
logger = logging.Logger('catch_all')

# get coin information
coindata, default_cols = extract_coindata()


def get_common_cols(db_cols, ext_cols):
    common_cols = []
    for col in ext_cols:
        if col == '24h':
             ext_cols[ext_cols.index(col)] = "Change_24h"
             col = "Change_24h"
        elif col == "Volume(24h)":
            ext_cols[ext_cols.index(col)] = "Volume_24h"
            col = "Volume_24h"
        if col in db_cols:
            common_cols.append(col)

    return common_cols    

def load_data(data = coindata,web_columns=default_cols, session=engine):
    """
    Loads data in batches into database
    
    returns None
    --------
    data: list of tuples of coin infomation scraped from websites
    
    web_columns: Website table headers
    
    session: a binded engine session maker
    """

    # start stopwatch
    start = datetime.now()
    
    # creating an instance of the session
    s = session.connect()

    
    query = """Select column_name from INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'transacttb'"""
    info =  s.execute(query).fetchall()
    db_cols = [i[0] for i in info] # Get the columns in the database table

    
    # getting columns. Columns must match columns on the crypto table in the database
    columns = get_common_cols(db_cols, web_columns)
    data_frame = pd.DataFrame(data, columns=web_columns)[columns]
    
    try:

        data_frame.to_sql('transacttb', con=s, if_exists='append', index = False)
        # stop stopwatch
        stop = datetime.now()  
        
        # print log info
        print('Batch Load Executed!!!')
        print("Total time: {} seconds".format(stop-start))
        
    # capturing exception and errors
    except Exception as e:
        logger.error(e, exc_info=True)
        print("Could not load data into database")
        
    
    finally:
        # make sure the session is close whether data was loaded or not
        s.close()
        
def csv_to_db(engine_session, df):
    
    """
    Loads data from csv file into database
    
    Returns None
    --------------
    engine_session: a started database engine session
    
    df: pd.DataFrame: a pandas dataframe object
    """
    try:
        assert type(df) == pd.DataFrame
        
        # getting columns.
        # Columns must correspond to table columns in the database
        columns = tuple(df.columns)
        
        # getting data in form or numpy arrays
        data = df.values
        
        # loading data into database
        load_data(data, columns, engine_session)
        
    except Exception as e:
        logger.error(e, exc_info=f'Expected df to be {pd.DataFrame} got {type(df)}\n Data could not be loaded')

    finally:
        # make sure the session is close whether data was loaded or not
        engine_session.close()



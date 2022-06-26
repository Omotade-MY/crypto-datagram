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
    from config import local_engine
    
except ModuleNotFoundError:
    from omotade.extract import extract_coindata
    from omotade.config import local_engine

from sqlalchemy.orm import sessionmaker
from tqdm.std import tqdm
import pandas as pd
from datetime import datetime
import logging
logger = logging.Logger('catch_all')

# get coin information
coindata, columns = extract_coindata()

#binding session to local engine
Session = sessionmaker(bind = local_engine)

def load_data(data = coindata,columns=columns, session=Session):
    """
    Loads data in batches into database
    
    returns None
    --------
    data: list of tuples of coin infomation scraped from websites
    
    columns: the attribute of the data corresponding to the columns in the database table
    
    session: a binded engine session maker
    """
    # start stopwatch
    start = datetime.now()
    
    # creating an instance of the session
    s = session()
    
    # getting columns. Columns must match columns on the crypto table in the database
    cols = columns
    
    try:
        for coin in tqdm(data, desc="Inserting data into Crypto table"):
            
            # convert coin information to list
            vals = list(coin)
            
            s.rollback()
            
            # sql query for inserting data into database
            query = '''INSERT INTO public.Crypto ({}, {}, {}, 
            "{}", {}, "{}", "{}", "{}", "{}", "{}", {}) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')'''\
            .format(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8], 
                    cols[9], cols[10], vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7],
                    vals[8], vals[9], vals[10])
            
            # executing query
            s.execute(query)
            
            # making sure query/changes are commited into database
            s.commit()
        
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
             
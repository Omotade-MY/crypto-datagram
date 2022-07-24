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
    from config import local_engine, cloudpb_engine, cloudtb_engine
    
except ModuleNotFoundError:
    from omotade.extract import extract_coindata
    from omotade.config import local_engine, cloudtb_engine, cloudpb_engine

engine = cloudtb_engine

from sqlalchemy.orm import sessionmaker
from tqdm.std import tqdm
import pandas as pd
from datetime import datetime
import logging
logger = logging.Logger('catch_all')

# get coin information
coindata, default_cols = extract_coindata()

#binding session to local engine
#Session = sessionmaker(bind = engine)

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


# =====================================================================================#
# Production Database related functions

def execute_query(engine, query, returns = False):

    """
        ------------
        returns None
        ------------
        This function executes an sql query on the a database engine

        engine: a  database engine
        query: SQL query to be executed
    """
    conn = engine.connect()

    try:
        rs = conn.execute(query)
    except Exception as e:
        logger.log(e)
        print('Unknown error occured')

    finally:
        conn.close()
    if returns == True:
        return rs.fetchall()
    

def get_websites(df):
    """
    -------------
    returns tuple
    -------------
    retunrs a sequence of unique websites present in a data

    df: a pandas dataframe object
    """
    webs = list(df['Website'].unique())
    if len(webs) <= 1:
        if 'END' not in webs:
            webs.append('END')
    return tuple(webs)


# get WebId
def get_web_id(engine):

    """
    ---------------------------
    returns dictionary of WebId
    ---------------------------
    This funtion checks and returns the web ids of websites present in the transact table.
    
    """
    query = f"""SELECT * FROM website
                Where website.url in {websites}"""
    webs = execute_query(engine, query=query, returns=True)
    if len(webs) != 0:
        urlid = {url:id for id,url in webs}
    else:
        urlid = {}
    return urlid



# get CoinId
def get_coin_id(engine, coinname):
    """
    ----------
    return int
    ----------

    checks and returns the coinid of a coin

    engine: A database engine
    coinname: name of the coin
    """
    query = f"""SELECT CoinId FROM coin
                Where coin.CoinName = '{coinname}'"""
    rs = execute_query(engine, query=query, returns=True)
    if len(rs) != 0:
        coinid = rs[0][0]
    else:
        coinid = 0
    return coinid


def update_web_id(ext_engine=transact_engine, ld_engine=engine):
    """
    ------------
    returns None
    ------------ 
    create a website Id for a website on that appears on the transact tb

    ext_enigine" tranct 
    """
    query = """SELECT DISTINCT "Website" from transacttb """
    rs = execute_query(ext_engine, query, returns=True)
    webs = [web[0] for web in rs]
    for web in webs:
        query = f"""INSERT INTO website (url)
                    select '{web}'
                    where not exists (select WebId from Website where
				    url = '{web}' ) """

        execute_query(ld_engine, query)


def update_coin(ext_engine=transact_engine, ld_engine=engine):

    """
        -------------
        returns None
        ------------

        upates the coin table
    """
    query = """SELECT DISTINCT "CoinName", "Symbol" from transacttb """
    rs = execute_query(ext_engine, query, returns=True)
    coins = [(coin, symbol)  for coin, symbol in rs]
    for coin, symbol in coins:
        
        query = f"""INSERT INTO coin (coinname, symbol)
                    select '{coin}', '{symbol}'
                    where not exists (select CoinId from coin where
				    coinname = '{coin}' ) """

        execute_query(ld_engine, query)


             
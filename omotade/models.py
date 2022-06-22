# -*- coding: utf-8 -*-
"""
Created on Tue May 17 22:29:48 2022

@author: Omotade
"""


from sqlalchemy import create_engine
import logging

logger = logging.Logger('catch_all')

def create_table(eng):
    query = ''' CREATE TABLE IF NOT EXISTS Crypto (ID serial PRIMARY KEY,
    CoinName VARCHAR (20) , Symbol VARCHAR (10), Time TIMESTAMP (40), Price VARCHAR (30),
    "1h" VARCHAR (10), "24h" VARCHAR (10), "7d" VARCHAR (10), "Volume(24h)" VARCHAR (30),
    "Circulating Supply" VARCHAR (40) , "Market Cap" VARCHAR (50),
    Website VARCHAR(200))'''
    
    conn = eng.connect()
    try:
        conn.execute(query)
        print('Table Created\nStatus: Successfull!!!')
    except Exception as e:
        logger.error(e, exc_info=True)
        print("Could not create table")
        
    finally:
        conn.close()


def drop_table(eng):
    query = ''' DROP TABLE IF EXISTS Crypto'''
    conn = eng.connect()
    try:
        conn.execute(query)
        print('Table dropped\nStatus: Successfull!!!')
    except Exception as e:
        logger.error(e, exc_info=True)
        print('Drop Table Failed')
        
    finally:
        conn.close()
        


def reset_database(eng):
    drop_table(eng)
    create_table(eng)
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 23 11:04:12 2022

@author: rahmah
"""

import pandas as pd 
import psycopg2

def load_csv_to_db():
    
      # open coonection
      conn = psycopg2.connect(database = 'postgres', user= 'postgres', password = 'rahmah', host ='localhost', port = '5432')
      
          
      cursor = conn.cursor()
      
         
      
      # create the table
      
      query =  '''CREATE TABLE CRYPTOCURRENCIES(ID SERIAL PRIMARY KEY, NAME VARCHAR(20),
      SYMBOL VARCHAR(10), TIME TIMESTAMP(50), PRICE VARCHAR(30), CHANGE_24H VARCHAR(10),
      CHANGE_7D VARCHAR(10), VOLUME_24H VARCHAR(30), MARKET_CAP VARCHAR(30), WEBSITE VARCHAR(50))'''
      cursor.execute(query)
  
    
     
    
      
      
      #load the csv data to the db
      
      my_file = open('cryptos.csv')

      SQL_STATEMENT ='''COPY CRYPTOCURRENCIES(name, symbol, time, price, change_24h, change_7d, volume_24h, market_cap,
      website) FROM 'C:/Users/Public/cryptos.csv' DELIMITER ',' CSV HEADER'''
      
          
      cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
        
           
     
          
      conn.commit()
          
      
      
      
       
      conn.close()
      
      
      
     
load_csv_to_db()








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
      
      try:
          
          cursor = conn.cursor()
      
          print('open db successful')
          
      except:
          print('open dbnot successful')
      
      # create the table
      
      query =  '''CREATE TABLE CRYPTOCURRENCIES(ID SERIAL PRIMARY KEY, NAME VARCHAR(20),
      SYMBOL VARCHAR(10), TIME TIMESTAMP(50), PRICE VARCHAR(30), CHANGE_24H VARCHAR(10),
      CHANGE_7D VARCHAR(10), VOLUME_24H VARCHAR(30), MARKET_CAP VARCHAR(30), WEBSITE VARCHAR(50))'''
      cursor.execute(query)
  
    
      #set time style
    
      time = '''SET datestyle = "dmy"'''
      cursor.execute(time)
      
      #load the csv data to the db
      
      my_file = open('cryptos.csv')

      SQL_STATEMENT ='''COPY CRYPTOCURRENCIES(name, symbol, time, price, change_24h, change_7d, volume_24h, market_cap,
      website) FROM 'C:/Users/Public/cryptos.csv' DELIMITER ',' CSV HEADER'''
      try:
          
          cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
          print('file copied to db')
           
      except:
          print('file not copied')
      
      #commit to the db
      try:
          
          conn.commit()
          print('table imported to db')  
          
      except:
          print('table not imported')
      
      
      
       
      cursor.close()
      
      
      
     
load_csv_to_db()








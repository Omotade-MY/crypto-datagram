# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 14:56:02 2022

@author: rahmah
"""

from bs4 import BeautifulSoup
import requests

import pandas as pd
import csv
from datetime import datetime

def status():
    #getting the website status
    res = requests.get('https://m.ng.investing.com/crypto/')
    status = res.status_code
    
    
status()





def scrape():
    """
    scraping the first five cryptocurrencies from m.ng.investing.com/crypto website 
    with their properties price symbol change in 24 hours change in 7 days volume in 24 hours
    and market cap"""
    
    #creaate list to store the data
    name = []
    symbol = []
    price = []
    change_1h = []
    change_24h =[]
    change_7d = []
    volume_24h = []
    market_cap = []
    
    
    res = requests.get('https://m.ng.investing.com/crypto/')
    soup = BeautifulSoup(res.content, 'html.parser')
    results = soup.select('tr', {'class': 'odd'})
   
    count = 0
    for result in results[1:]:
         if count == 6:
              break;
         count = count + 1
         names = result.select('td.left.bold.elp.name.cryptoName.first.js-currency-name')[0].get_text().strip()
         symbols = result.select('td.left.noWrap.elp.symb.js-currency-symbol')[0].get_text().strip()
         prices = result.select('td.price.js-currency-price')[0].get_text().strip()
         aday = result.select('td.js-currency-change-24h')[0].get_text().strip()
         aweek = result.select('td.js-currency-change-7d')[0].get_text().strip()
         volume = result.select('td.js-24h-volume')[0].get_text().strip()
         market = result.select('td.js-market-cap')[0].get_text().strip()
        
    
    
         # append to the list created
         
         name.append(names)
         symbol.append(symbols)
         price.append(prices)
    
         change_24h.append(aday)
         change_7d.append(aweek)
         volume_24h.append(volume)
         market_cap.append(market)
         
      # time the data was scraped   
    now = datetime.now()
    d = now.strftime('%Y-%m-%d %H:%M:%S')
    
    #creating the data frame
    
    crypto_df = pd.DataFrame()
    
    # saving the rows to the dataframe
    
    crypto_df['Name'] = name
    crypto_df['Symbol'] = symbol
    crypto_df['Time'] = d
    crypto_df['Price'] = price
    crypto_df['Change_24h'] = change_24h
    crypto_df['Cange_7d'] = change_7d
    crypto_df['Volume_24h'] = volume_24h
    crypto_df['Market_cap'] = market_cap
    crypto_df['Website'] = "m.ng.investing.com/crypto/"
    
    #saving tocsv file
    
    crypto_df.to_csv('cryptos.csv', index = False)    
scrape()







def csv_to_df(y):
    print(y)
    




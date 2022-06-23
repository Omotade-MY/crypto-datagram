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

#getting the current time aand date
now = datetime.now()
d = now.strftime('%d-%m-%Y %H:%M:%S')


# create list
crypto = []



def scrape():
    """
    scraping the first five cryptocurrencies from m.ng.investing.com/crypto website 
    with their properties price symbol change in 24 hours change in 7 days volume in 24 hours
    and market cap"""
    
    
    
    
    res = requests.get('https://m.ng.investing.com/crypto/')
    soup = BeautifulSoup(res.content, 'html.parser')
    results = soup.select('tr', {'class': 'odd'})
    now = datetime.now()
    d = now.strftime('%d-%m-%Y %H:%M:%S')
    count = 0
    for result in results[1:]:
         if count == 6:
              break;
         count = count + 1
         name = result.select('td.left.bold.elp.name.cryptoName.first.js-currency-name')[0].get_text().strip()
         symbol = result.select('td.left.noWrap.elp.symb.js-currency-symbol')[0].get_text().strip()
         price = result.select('td.price.js-currency-price')[0].get_text().strip()
         change_24h = result.select('td.js-currency-change-24h')[0].get_text().strip()
         change_7d = result.select('td.js-currency-change-7d')[0].get_text().strip()
         volume_24h = result.select('td.js-24h-volume')[0].get_text().strip()
         market_cap = result.select('td.js-market-cap')[0].get_text().strip()
        
    
    
         # append to the list created
         
         crypto.append({'Name':name, 'Symbol':symbol, 'Time':d,
                    'Price':price, 'Change_24h':change_24h,
                    'Change_7d':change_7d, 'Volume_24h':volume_24h,
                    'Market_cap': market_cap, 'website':"m.ng.investing.com/crypto/" })

    
         
scrape()



#creating the data frame

def cryptodf():
    
    
    keys = crypto[0].keys()

    with open('cryptos.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(crypto)
    

cryptodf()


def csv_to_df(y):
    print(y)
    




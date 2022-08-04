# -*- coding: utf-8 -*-
"""
Created on Mon May 16 19:54:44 2022

@author: Omotade
"""
import requests
from datetime import datetime
from bs4 import BeautifulSoup



def scrape():
    my_headers = {'User-Agent': '''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' + ' (KHTML, like Gecko) Chrome/61.0.3163.100Safari/537.36'''
    }
    response = requests.get('https://www.coingecko.com', headers=my_headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    print(response.status_code)
    return soup



def extract_coindata():



    # extract the data from website
    cryptosoup = scrape()

    # get time of extraction
    now = datetime.now()

    # mining data information
    table = cryptosoup.select('table')[0]
    headpane = table.select('thead')[0]
    body = table.select('tbody')[0]

    # A map for transforming column names
    col_map = {'Coin':'CoinName', 'Price':'Price', '24h':'Change_24h', '24h Volume':'Volume_24h',
           'Mkt Cap': 'Market Cap'}
    coin_info = []

    heads = headpane.select('th')[2:-1]
    columns = []
    for head in heads[:-1]:
        col = head.text.strip()
        if col in col_map.keys():

            column = col_map[col]
        elif col == '':
            column = "Symbol"


        else:
            column = col 
        columns.append(column)


    # select top 5 rows
    data = []
    row_vals = []
    coin_info = []
    rows = body.select('tr')[:5]
    for r in rows:
        row_vals = []

        row =  r.select('td')[2:-2]

        for i in range(len(row)):
            val = row[i]
            if i == 0:
                # get name and symbol
                temp = val.text.strip().split()

                try:
                    name, sign = temp
                except ValueError:
                    if "Coin" in temp:
                        sign = temp[-1]
                        name = ' '.join(temp[:-1])
                row_vals.append(name)
                row_vals.append(sign)

            else:
                val = val.text.strip()
                row_vals.append(val)

            col = columns[i]



        time = now.strftime('%Y/%m/%d %H:%M')  

        # insert time at at 3rd column
        row_vals.insert(2,time)

        try:
            row_vals.remove('')

        except ValueError:
            pass

        row_vals.append('https://www.coingecko.com/')

        info = tuple(row_vals)
        coin_info.append(info)
        #print(columns,'\n',row_vals)

    columns.insert(2,'Time')
    columns.append('Website')


    return coin_info, columns


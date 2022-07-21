# import necessasary
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime as dt
import csv

# store website in variable
website = 'https://www.coingecko.com/'

# Get Request
response = requests.get(website)
response.status_code

# status code
response.status_code

# Soup Object
soup = BeautifulSoup(response.content, 'html.parser')

# result
results = soup.find('table', {"class": "table-scrollable"}).find('tbody').find_all('tr')
len(results)

# target necessasary data

# Name
results[0].find('a', {'class': 'tw-hidden lg:tw-flex font-bold tw-items-center tw-justify-between'}).get_text().strip()

# Symbol
results[0].find('span', {'class': 'tw-hidden d-lg-inline font-normal text-3xs ml-2'}).get_text().strip()

# Price
results[0].find('td', {'class': 'td-price price text-right pl-0'}).get_text().strip()

# 1h change
results[0].find('td', {'class': 'td-change1h change1h stat-percent text-right col-market'}).get_text().strip()

# 24h change
results[0].find('td', {'class': 'td-change24h change24h stat-percent text-right col-market'}).get_text().strip()

# 7days change
results[0].find('td', {'class': 'td-change7d change7d stat-percent text-right col-market'}).get_text().strip()

# 24hr volume
results[0].find('td', {'class': 'td-liquidity_score lit text-right col-market'}).get_text().strip()

# Market cap
results[0].find('td', {'class': 'td-market_cap cap col-market cap-price text-right'}).get_text().strip()

# Put everything inside a for loop
name = []
symbol = []
price = []
change_1h = []
change_24h = []
change_7d = []
volume_24h = []
market_cap = []

for result in results:
    # name
    try:
        name.append(result.find('a', {
            'class': 'tw-hidden lg:tw-flex font-bold tw-items-center tw-justify-between'}).get_text().strip()
                    )
    except:
        name.append('n/a')
    # symbol
    try:
        symbol.append(
            result.find('span', {'class': 'tw-hidden d-lg-inline font-normal text-3xs ml-2'}).get_text().strip())
    except:
        symbol.append('n/a')
    # price
    try:
        price.append(result.find('td', {'class': 'td-price price text-right pl-0'}).get_text().strip())
    except:
        price.append('n/a')
    # change_1h
    try:
        change_1h.append(
            result.find('td', {'class': 'td-change1h change1h stat-percent text-right col-market'}).get_text().strip())
    except:
        change_1h.append('n/a')
    # change_24h
    try:
        change_24h.append(result.find('td', {
            'class': 'td-change24h change24h stat-percent text-right col-market'}).get_text().strip())
    except:
        change_24h.append('n/a')
    # change_7d
    try:
        change_7d.append(
            result.find('td', {'class': 'td-change7d change7d stat-percent text-right col-market'}).get_text().strip())
    except:
        change_7d.append('n/a')
    # volume_24h
    try:
        volume_24h.append(
            result.find('td', {'class': 'td-liquidity_score lit text-right col-market'}).get_text().strip())
    except:
        volume_24.append('n/a')
    # market_cap
    try:
        market_cap.append(
            result.find('td', {'class': 'td-market_cap cap col-market cap-price text-right'}).get_text().strip())
    except:
        market_cap.append('n/a')

# Time
from datetime import datetime

time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create pandas dataframe
crypto_df = pd.DataFrame(
    {'Name': name, 'Symbol': symbol, 'Time': time, 'Price': price, "Change_1h": change_1h, 'Change_24h': change_24h,
     'Change_7d': change_7d, 'Volume_24h': volume_24h, 'Market_cap': market_cap, 'Website': website})
crypto = crypto_df[:5]
crypto.to_csv('crypto', index=True)
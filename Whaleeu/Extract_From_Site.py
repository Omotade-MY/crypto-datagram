
import pandas as pd
from bs4 import BeautifulSoup
import time
import datetime
import requests
import re




#Function for scraping and convert to csv
my_headers = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' + ' (KHTML, like Gecko) Chrome/61.0.3163.100Safari/537.36'
}
u ="https://finance.yahoo.com/cryptocurrencies/"
header = []
data = []

def Scrape(test_url=u):
    """ Function for scraping data from website and conversion to csv file"""
    r =  requests.get(test_url, headers = my_headers)
    html_resrponse = r.text
    soup = BeautifulSoup(html_resrponse,"html.parser")
    
    #head for column name
    table_h = soup.find("table")
    for i in table_h.find_all("th"):
        title = i.text
        header.append(title)

     #cryptocurrency scrape
    table = soup.find("tbody")       
    for row in table.find_all("tr")[:]:
        item_in_row = row.find_all("td")
        rows =  [table1.get_text() for table1 in item_in_row]
        data.append(rows[:10])
    
    #Creating data frame
    Dframe = pd.DataFrame(data[:5],columns = header[:10]) 
    Dframe["Time"] = datetime.datetime.now()
    Dframe["Site_Url"] = test_url
    Dframe["Name"] = Dframe["Name"].apply(lambda x : re.split(r'(\s)', x)[0])
    Dframe["Symbol"] = Dframe["Symbol"].apply(lambda x : x.split("-")[0])
    Dframe.rename(columns = {'Price (Intraday)':'Price','% Change':'Percentage_Change','Volume in Currency (Since 0:00 UTC)':'Volume_in_Currency_Since_0:00_UTC',"Volume in Currency (24Hr)" :"Volume_in_Currency_in_24Hr", "Total Volume All Currencies (24Hr)":"Total_Volume_All_Currencies_in_24Hr"}, inplace = True)
    return Dframe
# Scrape()
# To csv
Scrape().to_csv("crypto_data.csv", index=False)


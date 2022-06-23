# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 18:07:18 2022

@author: rahmah
"""

def rundf():
    import pandas as pd
    from cryp import csv_to_df
    df = pd.read_csv('cryptos.csv')
    
    csv_to_df(df)
    
    
rundf()
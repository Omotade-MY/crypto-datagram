# -*- coding: utf-8 -*-
"""
Created on Tue May 17 22:32:11 2022

@author: Omotade
"""
import sys
import os
sys.path.append(os.getcwd())

from sqlalchemy import create_engine

from auths import hostname, password


#LOCAL_DATABASE_URI = "postgresql+psycopg2://postgres:udkhulbisalaam@localhost:5432/Cryptocurrency"

LOCAL_DATABASE_URI_PB = "postgresql+psycopg2://postgres:"+password+"@"+hostname+":5432/Cryptocurrency"
LOCAL_DATABASE_URI_TB = "postgresql+psycopg2://postgres:"+password+"@"+hostname+":5432/postgres"


localtb_engine = create_engine(LOCAL_DATABASE_URI_TB)
localpb_engine = create_engine(LOCAL_DATABASE_URI_PB)


    

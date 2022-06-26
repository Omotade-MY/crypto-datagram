# -*- coding: utf-8 -*-
"""
Created on Tue May 17 22:32:11 2022

@author: Omotade
"""
from sqlalchemy import create_engine

#from auths import hostname, password


LOCAL_DATABASE_URI = "postgresql+psycopg2://postgres:udkhulbisalaam@localhost:5432/Cryptocurrency"

#CLOUD_DATABASE_URL = "postgresql+psycopg2://postgres:"+password+"@"+hostname+":5432/Cryptocurrency"

local_engine = create_engine(LOCAL_DATABASE_URI)
#cloud_engine = create_engine(CLOUD_DATABASE_URL)


    

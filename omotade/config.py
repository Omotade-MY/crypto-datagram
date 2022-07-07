# -*- coding: utf-8 -*-
"""
Created on Tue May 17 22:32:11 2022

@author: Omotade
"""
from sqlalchemy import create_engine

from auths import hostname, password


LOCAL_DATABASE_URI = "postgresql+psycopg2://postgres:udkhulbisalaam@localhost:5432/Cryptocurrency"

CLOUD_DATABASE_URI_TB = "postgresql+psycopg2://omotade:"+password+"@"+hostname+":5432/CryptoTransactDB"
CLOUD_DATABASE_URI_PB = "postgresql+psycopg2://omotade:"+password+"@"+hostname+":5432/ProductionDB"

local_engine = create_engine(LOCAL_DATABASE_URI)
cloudtb_engine = create_engine(CLOUD_DATABASE_URI_TB)
cloudpb_engine = create_engine(CLOUD_DATABASE_URI_PB)


    

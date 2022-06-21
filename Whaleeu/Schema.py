import pandas as pd



def schema(extraction,conection):
    '''This function create a schema for Db'''
    pd.io.sql.get_schema(extraction ,name = "crypto",con=conection,)                 
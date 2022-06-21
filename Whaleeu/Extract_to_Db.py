
import pandas as pd



def Input_to_Db(extract,conection):
    '''This function get extract and load to Db'''
    extract.to_sql('crypto', con = conection, index = False, if_exists = "append")
    print("insert another")
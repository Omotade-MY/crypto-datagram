from  DB_Creation import create_Db
from Extract_From_Site import Scrape
from Schema import schema
from Extract_to_Db import Input_to_Db
import time



extract = Scrape()
conect = create_Db('postgres://postgres:postgres@localhost/postgres')
schema(extract,conect)

Input_to_Db(extract,conect)
time.sleep(3600)


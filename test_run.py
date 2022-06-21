from Whaleeu.Schema import  schema
from omotade.extract import extract_coindata
from omotade.crud import load_data

data, cols = extract_coindata()

# load to database

load_data(data, cols)




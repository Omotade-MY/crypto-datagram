from extract import extract_coindata, to_csv
from crud import load_data

# extract coin information

data, cols = extract_coindata()

# save to csv file

to_csv(data, cols)

# load to database

load_data(data, cols)

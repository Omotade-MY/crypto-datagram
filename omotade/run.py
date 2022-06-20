from extract import extract_coindata, to_csv

# extract coin information

data, cols = extract_coindata()

# save to csv file

to_csv(data, cols)

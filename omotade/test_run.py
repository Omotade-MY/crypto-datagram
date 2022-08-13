from product_db_etl import  extract, transform, load_prd_db
from config import localtb_engine, localpb_engine


# Extract

datacoin = extract(localtb_engine, 'BTC', migrate=True)
print('This is datacoin',datacoin)

# Transaform

transformed_data = transform(datacoin, 'bitcoin', localpb_engine)

# Load

load_prd_db(transformed_data, 'bitcoin', localpb_engine)


## perform ETL operation for bnb coin

# Extract
print("Exectuting on bnb table")

datacoin_bnb = extract(localtb_engine, 'BNB')


 
# Transaform

transformed_data = transform(datacoin_bnb, 'bnb', localpb_engine)

# Load

load_prd_db(transformed_data, 'bnb', localpb_engine)


## perform ETL operation for usd coin

# Extract
print("Exectuting on usdc table")

datacoin_usd = extract(localtb_engine, 'USDC')

 
# Transaform

transformed_data = transform(datacoin_usd, 'usdc', localpb_engine)

# Load

load_prd_db(transformed_data, 'usdc', localpb_engine)

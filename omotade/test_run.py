from product_db_etl import  extract, transform, load_prd_db
from config import local_engine, cloudpb_engine, cloudtb_engine


# Extract

datacoin = extract(cloudtb_engine, 'BTC')


# Transaform

transformed_data = transform(datacoin, 'bitcoin', cloudpb_engine)

# Load

load_prd_db(transformed_data, 'bitcoin', cloudpb_engine)


## perform ETL operation for bnb coin

# Extract
print("Exectuting on bnb table")

datacoin_bnb = extract(cloudtb_engine, 'BNB')

 
# Transaform

transformed_data = transform(datacoin_bnb, 'bnb', cloudpb_engine)

# Load

load_prd_db(transformed_data, 'bnb', cloudpb_engine)


## perform ETL operation for usd coin

# Extract
print("Exectuting on usdc table")

datacoin_usd = extract(cloudtb_engine, 'USDC')

 
# Transaform

transformed_data = transform(datacoin_usd, 'usdc', cloudpb_engine)

# Load

load_prd_db(transformed_data, 'usdc', cloudpb_engine)

from auths import hostname, password
from product_db_etl import  extract, transform, load_prd_db, get_web_id
from sqlalchemy import create_engine
from config import local_engine, cloudpb_engine, cloudtb_engine


# Extract

datacoin = extract(cloudtb_engine, 'USD Coin')


# Transaform

transformed_data = transform(datacoin, 'usdc', local_engine)

# Load

load_prd_db(transformed_data, 'usdc', local_engine)




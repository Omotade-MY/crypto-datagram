from auths import hostname, password
from product_db_etl import  extract, transform, load_prd_db, get_web_id
from sqlalchemy import create_engine

PRD_DATABASE_URI = f'postgresql+psycopg2://omotade:{password}@{hostname}:5432/ProductionDB'
TRNS_DATABASE_URI = f'postgresql+psycopg2://omotade:{password}@{hostname}:5432/CryptoTransactDB'
DATABASE_URI = f'postgresql+psycopg2://postgres:udkhulbisalaam@localhost:5432/Cryptocurrency'

engine = create_engine(DATABASE_URI)
transact_engine = create_engine(TRNS_DATABASE_URI)


# Extract

datacoin = extract(transact_engine, 'Bitcoin')

# get web ids
#webs = get_web_id()

# Transaform

transformed_data = transform(datacoin, 'bitcoin', engine)

# Load

load_prd_db(transformed_data, 'bitcoin', engine)




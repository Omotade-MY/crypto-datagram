from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2

Database_uri = "postgres+psycopg2://postgres:08080935308@localhost:5432/postgres"
engine = create_engine(Database_uri)
connection = engine.connect()
def create_table(eng):
    query = '''CREATE TABLE IF NOT EXISTS Cryptotb (ID serial PRIMARY KEY,
    Name VARCHAR(20), Symbol VARCHAR(10), Time TIMESTAMP(40), Price VARCHAR(30), Change_1h VARCHAR(10), Change_24h VARCHAR(10), Change_7d VARCHAR(10), Volume_24h VARCHAR(30), Market_cap VARCHAR(30), Website VARCHAR(200))'''

    connection.execute(query)
    print('Table created')
create_table(engine)
query = '''select count(*) from public.Cryptotb'''
connection.execute(query).fetchall()

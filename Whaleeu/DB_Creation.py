
from sqlalchemy import create_engine
import psycopg2
def create_Db(conn_string):
    db = create_engine(conn_string)
    conn = db.connect()
    return conn
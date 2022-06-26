

def create_transact_table(engine, table_name):
    conn = engine.connect()
    query = f'''CREATE TABLE IF NOT EXISTS {table_name} (TransactId Serial PRIMARY KEY, 
            CoinName VARCHAR (15), Symbol VARCHAR (10), Time Timestamp (40), Price VARCHAR (30),
            "Change(24h)" VARCHAR (5), "Volume(24h)" VARCHAR (35), "Market Cap" VARCHAR (40), Website VARCHAR(200))
            '''
    try:
        conn.execute(query)
        print('Table Created successfully')
    except Exception as e:
        print("Could not create table\n",e)
    finally:
        conn.close()

def create_coin_tables(engine, coins):

    

def drop_table(engine, table_name):
    query = f'''DROP TABLE IF EXISTS {table_name} '''
    conn = engine.connect()
    try:
        conn.execute(query)
        print('Table dropped successfully')
    except Exception as e:
        print('Could not drop table \n', e)
    finally:
        conn.close()
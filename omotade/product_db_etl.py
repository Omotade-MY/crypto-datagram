# =====================================================================================#
# Production Database related functions
from config import local_engine, cloudpb_engine, cloudtb_engine
import pandas as pd
from logging import Logger
logger = Logger('catch_all')
def execute_query(engine, query, returns = False):

    """
        ------------
        returns None
        ------------
        This function executes an sql query on a database engine

        engine: a  database engine
        query: SQL query to be executed
    """
    conn = engine.connect()

    try:
        rs = conn.execute(query)
    except Exception as e:
        logger.log(e)
        print('Unknown error occured')

    finally:
        conn.close()
    if returns == True:
        return rs.fetchall()
    

def extract(engine, coinname, migrate=False):
        query = """Select column_name from INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'transacttb'"""

        info =  execute_query(engine, query=query, returns=True)
        column_names = [i[0] for i in info]

        if migrate == False:
            query = f"""SELECT * FROM TRANSACTTB AS TB
                WHERE TB."CoinName" = '{coinname}' and TB."Time" >= now() - interval '10 minutes'"""
                
        elif migrate == True:
            query = f"""SELECT * FROM TRANSACTTB AS TB
                WHERE TB."CoinName" = '{coinname}' """

        coin = pd.DataFrame(execute_query(transact_engine, query=query, returns=True), columns=column_names)

        print('Extraction Done!!!\n')

        return coin

# Update website table

def update_web_id(ext_engine = cloudtb_engine, ld_engine = cloudpb_engine):
    """
    ------------
    returns None
    ------------ 
    create a website Id for a website on that appears on the transact tb

    ext_enigine" tranct 
    """
    query = """SELECT DISTINCT "Website" from transacttb """
    rs = execute_query(ext_engine, query, returns=True)
    webs = [web[0] for web in rs]
    for web in webs:
        query = f"""INSERT INTO website (url)
                    select '{web}'
                    where not exists (select WebId from Website where
				    url = '{web}' ) """

        execute_query(ld_engine, query)


# Update coin table
def update_coin(ext_engine = cloudtb_engine, ld_engine=cloudpb_engine):

    """
        -------------
        returns None
        ------------

        upates the coin table
    """
    query = """SELECT DISTINCT "CoinName", "Symbol" from transacttb """
    rs = execute_query(ext_engine, query, returns=True)
    coins = [(coin, symbol)  for coin, symbol in rs]
    for coin, symbol in coins:
        
        query = f"""INSERT INTO coin (coinname, symbol)
                    select '{coin}', '{symbol}'
                    where not exists (select CoinId from coin where
				    coinname = '{coin}' ) """

        execute_query(ld_engine, query)

# get WebId
def get_web_id(coin_df, engine = local_engine ):

    """
    ---------------------------
    returns dictionary of WebId
    ---------------------------
    This funtion checks and returns the web ids of websites present in the transact table.
    
    """
    webs = list(coin_df['Website'].unique())
    if len(webs) <= 1:
        if 'END' not in webs:
            webs.append('END')
    websites =  tuple(webs)


    query = f"""SELECT * FROM website
                Where website.url in {websites}"""
    webs = execute_query(engine, query=query, returns=True)
    if len(webs) != 0:
        urlid = {url:id for id,url in webs}
    else:
        update_web_id(ld_engine=engine)
        urlid = get_web_id(coin_df)
    return urlid




# get CoinId
def get_coin_id(coinname, engine= local_engine):
    """
    ----------
    return int
    ----------

    checks and returns the coinid of a coin

    engine: A database engine
    coinname: name of the coin
    """
    query = f"""SELECT CoinId FROM coin
                Where coin.CoinName = '{coinname}'"""
    rs = execute_query(engine, query=query, returns=True)
    if len(rs) != 0:
        coinid = rs[0][0]
    else:
        update_coin(ld_engine=engine)

        coinid = get_coin_id(coinname)
    return coinid





def adjust_data(df, engine, table_name='bitcoin'):
    query = f"""Select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' """
    temp =  engine.execute(query).fetchall()
    columns = [col[0] for col in temp ]
    columns.remove('ProductId')
    data = df[columns]

    return data

def clean(df):

    """
    returns a DataFrame of cleaned data

    """
    from warnings import filterwarnings
    filterwarnings('ignore')

    # clean price

    def rem(p):
        temp = p.replace(',', '')
        p = temp.strip('$')
        return float(p)
    df['Price'] = df['Price'].apply(rem)

    # clean volume

    def normal(vol):
        temp = vol.replace(',', '')
        vol = temp.strip('$')

        if 'B' in vol:
            vol = float(vol.strip('B'))
            vol = vol * 1000000000

        elif 'M' in vol:
            vol = float(vol.strip('M'))
            vol = vol * 1000000

        else:
            vol = float(vol)

        return vol

    df['Volume_24h'] = df['Volume_24h'].apply(normal)

    # clean Market cap
    df['Market Cap'] = df['Market Cap'].apply(normal)


    # clean 24 hours percentage change
    def rem_sign(perc):
        temp = perc.strip('%')
        try:
            perc = float(temp)
            return perc
        except ValueError as ve:
            logger.log(ve)
            print(f"{temp} could not be converted to float")
            return temp

        
            
    df['Change_24h'] = df['Change_24h'].apply(rem_sign)


    return df

             


def transform(coindf, target_table, engine):
    coindf['CoinId'] = coindf['CoinName'].apply(get_coin_id)
    coindf['WebId'] = coindf['Website'].map(get_web_id(coindf))

    adjusted_data = adjust_data(df=coindf,engine=engine, table_name=target_table)
    cleaned_data = clean(adjusted_data)

    print('Transformation Complete!!!\n')
    return cleaned_data
    
def load_prd_db(data, table, engine):
    """
    ------------
    returns None
    ------------

    Loads data into table on the production database 

    data: Dataframe : coin data
    table: str : name of target table
    engine: Engine: a database engine
    """
    try:
        assert type(data) == pd.DataFrame

    except AssertionError as e:
        logger.log(e)
        print(f"Type of data must be {pd.DataFrame}\n Trying to convert data to DataFrame")

        try:
            data = pd.DataFrame(data)
        except:
            
            print("failed to convert to DataFrame")

            return
    conn = engine.connect()
    try:

        data.to_sql(table, conn, if_exists='append', index=False)
        print(f'\nBatch Load into {table} table completed')

    except Exception as e:
        logger.log(e)
        print('Failed to load Database')
    
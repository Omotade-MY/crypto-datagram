from extract import extract_coindata
from crud import load_data
import time


# set timer
start_time = time.time()
interval = 180 # 180 secs = 3 mins
stop_time = start_time + interval
now = time.time() # get current time as floating number counts since the epoch
sleep_time = 3600 # 3600 secs =  2 hours
while now < stop_time:
    
    # extract coin information
    data, cols = extract_coindata()

    # load to database
    load_data(data, cols)
    now = time.time()
    time.sleep(sleep_time)

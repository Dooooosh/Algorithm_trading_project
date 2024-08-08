import pandas as pd
import requests
import schedule
import time as time_module
from datetime import datetime
import os
import json
import pytz

#Configuration
api_url = 'https://fapi.binance.com/fapi/v1/openInterest'
symbols = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'aaveusdt', 'avaxusdt', 'dogeusdt', 'linkusdt', 'trxusdt', 'xrpusdt']
output_dir = '/Users/Ohm/My_Algoes/data-streams/csv_files'
output_filename = os.path.join(output_dir, 'open_interest.csv')
oi_total_csv = os.path.join(output_dir, 'oi_total.csv')


# Check if the output directory exists, if not, create it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


#Function to write headers if they are missing
def write_headers():
    if not os.path.isfile(output_filename) or os.stat(output_filename).st_size == 0:
        with open(output_filename, 'w') as f:
            f.write(",".join([
                "time", "symbol", "openInterest", "price", "value"
            ]) + "\n")
            
def ask_bid(symbol):
    
    url = 'https://api.hyperliquid.xyz/info'
    headers = {'Content-Type': 'application/json'}
    
    data = {
        'type':'l2Book',
        'coin': symbol
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    l2_data = response.json()
    l2_data = l2_data['levels']
    #print(l2_data)
    
    #get bid and ask
    bid = float(l2_data[0][0]['px'])
    ask = float(l2_data[1][0]['px'])
    
    return ask, bid, l2_data

def fetch_open_interest():
    data = []
    total = 0 #Variable to store the total value of all symbols
    bkk_tz = pytz.timezone('Asia/Bangkok')
    
    for symbol in symbols:
        try:
            response = requests.get(api_url, params={'symbol': symbol.upper()})
            response.raise_for_status()
            result = response.json()
            
            open_interest = float(result['openInterest'])
            timestamp = datetime.fromtimestamp(result['time'] / 1000, bkk_tz)
            
            # Get the ask price
            ask_price, _, _ = ask_bid(symbol.replace('usdt', '').upper())
            
            # Calculate the value
            value = open_interest * ask_price
            
            data.append([timestamp, symbol.upper(), open_interest, ask_price, value])
            
        except Exception as e:
            print(f'Error fetching data for {symbol}: {e}')
            
    # Append new data to csv
    if data:
        df = pd.DataFrame(data, columns=['time', 'symbol', 'openInterest', 'price', 'value'])
        df.set_index('time', inplace=True)
        write_headers()  # Ensure headers are written before appending the data
        df.to_csv(output_filename, mode='a', header=False)
        
        # Calculate the total value
        total = sum([item[4] for item in data])
        
        # Get current time in Bangkok timezone
        current_time = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M')
        
        print(f'{current_time}: All symbols total open interest: ${total:,.0f}')
        
        # Save the total open interest to a new file oi_total.csv using pandas
        df_total = pd.DataFrame([total], columns=['total'])
        
        # Add the time to the df
        df_total['time'] = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M:%S')
        df_total.set_index('time', inplace=True)
        
        # Write the df to a csv file
        df_total.to_csv(oi_total_csv, mode='a', header=False)
        
        return df, data, total
    
def job():
    fetch_open_interest()
    
# Schedule the job every 1 minute
# Initial run to ensure data is fetched immediately when script starts
job()

schedule.every(1).minutes.do(job)

# Run the scheduled tasks
while True:
    try:
        schedule.run_pending()
        time_module.sleep(1)
    except Exception as e:
        print(f'Error in the scheduling loop: {e}')
        time_module.sleep(5)
import numpy as np
import pandas as pd

#File path
file_path = 'C:\Users\Ohm\Algo_Bootcamp\dev\data-streams\csv_files\bnb_liq.csv'

#Read the CSV file into a DataFrame
df = pd.read_csv(file_path, header=None)

#Specify columns names
df.columns = [
    'symbol', 'side', 'order_type', 'time_in_force',
    'original_quantity', 'price', 'average_price', 'order_status',
    'order_last_filled_quantity', 'order_filled_accumulated_quantity',
    'order_trade_time', 'usd_size'
]

#Function to datetime liquidation type
def datetime_liq_side(row):
    #Keep symbols with exactly 3 letters
    if (len(row['symbol']) == 3 or row['symbol'] == '1000PEPE') and row['usd_size'] > 3000:
        if row['side'] == 'SELL':
            return "L LIQ"
        elif row['side'] == 'BUY':
            return "S LIQ"
    return None

#Add LIQ SIDE column
df['LIQ_SIDE'] = df.apply(datetime_liq_side, axis=1)

#Filter out rows where LIQ_SIDE is NaN
df = df[df['LIQ_SIDE'].notna()]

#Convert epoch to datetime
df['datetime'] = pd.to_datetime(df['order_trade_time'], unit='ms')
df['datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))

#Set 'datetime' as index
df.set_index('datetime', inplace=True)

#Adjust symbol handling
df['symbol'] = df['symbol'].apply(lambda x:x if x == '1000PEPE' else x[:3])

#List of symbols
symbols = ['BTC', 'ETH', 'SOL', 'WIF', '1000PEPE']

#Filter and save the DatafRAME for each symbol
for symbol in symbols:
    #Filter DataFrame by symbol
    filtered_df = df[df['symbol'].str.startswith(symbol)]
    
    #Save the filtered DataFrame to a new CSV file
    output_path = f'/Users/Ohm/Algo_Bootcamp/dev/data-streams/{symbol}_liq.csv'
    filtered_df.to_csv(output_path)
    
#Reset index for sampling
df_all = df.reset_index()
df_all['datetime'] = pd.to_datetime(df_all['datetime'])
df_all.set_index('datetime', inplace=True)

#Separate resample for L LIQ and S LIQ
df_totals_L = df_all[df_all['LIQ_SIDE'] == 'L LIQ'].resample('5T').agg({'usd_size': 'sum', 'price':'mean'})
df_totals_S = df_all[df_all['LIQ_SIDE'] == 'S LIQ'].resample('5T').agg({'usd_size': 'sum', 'price':'mean'})

#Add LIQ_SIDE column back
df_totals_L['LIQ_SIDE'] = 'L LIQ'
df_totals_S['LIQ_SIDE'] = 'S LIQ'

#Combine the dataframe
df_totals = pd.concat([df_totals_L, df_totals_S])

#Add symbol column set to 'All'
df_totals['symbol'] = 'All'

#Reset index to have 'datetime' as a column
df_totals.reset_index(inplace=True)

#Reorder the DataFrame columns
df_totals = df_totals[['datetime', 'symbol', 'LIQ_SIDE', 'price', 'usd_size']]

#Save the aggregated totals DataFrame
output_totals_path = '/Users/Ohm/Algo_Bootcamp/dev/data-streams'
df_totals.to_csv(output_totals_path, index=False)

#Ensure the 'totals' DataFrame is generated properly
print(df_totals.head())


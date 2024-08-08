import pandas as pd
from datetime import datetime, timedelta
import pytz
from termcolor import cprint
import schedule, time
import os

# Example usage
timeframe = '15m'
output_dir = '/Users/Ohm/My_Algoes/data-streams/csv_files'
output_filename = os.path.join(output_dir, 'liquidation_summary.csv')

def calculate_liquidations(timeframe):
    # Configuration variables
    input_filename = os.path.join(output_dir, "binance_liquidations.csv")
    
    # Define column names based on the example you provided
    column_names = [
        'symbol', 'side', 'order_type', 'time_in_force',
        'original_quantity', 'price', 'average_price', 'order_status',
        'order_last_filled_quantity', 'order_filled_accumulated_quantity',
        'order_trade_time', 'usd_size'
    ]
    
    # Read CSV file into DataFrame, assuming it has a header row
    df = pd.read_csv(input_filename)
    
    # Convert the 'order_trade_time' column to datetime in UTC
    try:
        df['order_trade_time'] = pd.to_datetime(df['order_trade_time'], unit='ms', utc=True)
    except Exception as e:
        print(f"Error converting 'order_trade_time' to datetime: {e}")
        return None, 0, 0, 0

    df['usd_size'] = pd.to_numeric(df['usd_size'], errors='coerce')
    
    # Convert the 'order_trade_time' column to your local timezone (BKK)
    df['order_trade_time'] = df['order_trade_time'].dt.tz_convert('Asia/Bangkok')
    
    # Determine the maximum timestamp in the data
    max_timestamp = df['order_trade_time'].max()
    
    # Calculate the start time for the given interval
    if timeframe == '5m':
        start_time = max_timestamp - timedelta(minutes=5)
    elif timeframe == '15m':
        start_time = max_timestamp - timedelta(minutes=15)
    elif timeframe == '30m':
        start_time = max_timestamp - timedelta(minutes=30)
    elif timeframe == '1h':
        start_time = max_timestamp - timedelta(hours=1)
    elif timeframe == '90m':
        start_time = max_timestamp - timedelta(minutes=90)
    elif timeframe == '2h':
        start_time = max_timestamp - timedelta(hours=2)
    elif timeframe == '4h':
        start_time = max_timestamp - timedelta(hours=4)
    else:
        raise ValueError("Invalid timeframe specified")
    
    # Calculate the total amount of liquidations for the interval
    interval_df = df[(df['order_trade_time'] >= start_time) & (df['order_trade_time'] <= max_timestamp)]
    total_liquidation = interval_df['usd_size'].sum()
    long_liquidation = interval_df[interval_df['side'] == 'SELL']['usd_size'].sum()
    short_liquidation = interval_df[interval_df['side'] == 'BUY']['usd_size'].sum()
    
    # Output the results to a CSV file
    results = {
        'Interval': [timeframe],
        'Total Liquidation': [total_liquidation],
        'Long Liquidation': [long_liquidation],
        'Short Liquidation': [short_liquidation]
    }
    
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_filename, mode='a', index=False, header=not os.path.isfile(output_filename))
    
    return results, total_liquidation, long_liquidation, short_liquidation

def bot():
    print('')
    results, total_liquidation, long_liquidation, short_liquidation = calculate_liquidations(timeframe)
    print(f'These are the total liquidations {total_liquidation} in the last {timeframe}')
    print(f'These are the long liquidations {long_liquidation} in the last {timeframe}')
    print(f'These are the short liquidations {short_liquidation} in the last {timeframe}')
    
    if total_liquidation > 15000000:
        print('Total liquidations are greater than 15,000,000')

print('Running algo...')

bot()

schedule.every(15).seconds.do(bot)

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(f'Encountered an error: {e}')
        time.sleep(10)

import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

websocket_url = 'wss://fstream.binance.com/ws/!forceOrder@arr'
output_dir = '/Users/Ohm/My_Algoes/data-streams/csv_files'
filename = os.path.join(output_dir, 'binance_bigliqs.csv')

# Check if the output directory exists, if not, create it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

if not os.path.isfile(filename):
    with open(filename, 'w') as f:
        f.write(",".join([
            'symbol', 'side', 'order_type', 'time_in_force',
            'original_quantity', 'price', 'average_price', 'order_status',
            'order_last_filled_quantity', 'order_filled_accumulated_quantity', 
            'order_trade_time', "usd_size"
        ])+ "\n")

async def binance_liquidation(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                msg = await websocket.recv()
                order_data = json.loads(msg)['o']
                symbol = order_data['s'].replace('USDT', '')
                side = order_data['S']
                timestamp = int(order_data['T'])
                filled_quantity = float(order_data['z'])
                price = float(order_data['p'])
                usd_size = filled_quantity * price
                bkk_tz = pytz.timezone('Asia/Bangkok')
                time_bkk_tz = datetime.fromtimestamp(timestamp/1000, bkk_tz).strftime('%H:%M:%S')

                if usd_size > 100000:
                    liquidation_type = 'L LIQ' if side == 'SELL' else "S LIQ"
                    symbol = symbol[:4]
                    color = 'blue' if side == 'SELL' else 'magenta'
                    attrs = ['bold']

                    if usd_size > 500000:  # Very large liquidations
                        stars = '*' * 3
                        attrs.append('blink')
                        usd_size_million = usd_size / 1000000
                        output = f'{stars} {liquidation_type} {symbol} {time_bkk_tz} ${usd_size_million:.2f}m'
                        for _ in range(4):
                            cprint(output, 'white', f'on_{color}', attrs=attrs)
                    elif usd_size > 250000:  # Large liquidations
                        stars = '*' * 2
                        attrs.append('blink')
                        usd_size_million = usd_size / 1000000
                        output = f'{stars} {liquidation_type} {symbol} {time_bkk_tz} ${usd_size_million:.2f}m'
                        for _ in range(3):
                            cprint(output, 'white', f'on_{color}', attrs=attrs)
                    elif usd_size > 100000:  # Significant liquidations
                        stars = '*' * 1
                        usd_size_million = usd_size / 1000000
                        output = f'{stars} {liquidation_type} {symbol} {time_bkk_tz} ${usd_size_million:.2f}m'
                        for _ in range(2):
                            cprint(output, 'white', f'on_{color}', attrs=attrs)

                    # Log to file
                    msg_values = [str(order_data.get(key)) for key in ['s', 'S', 'o', 'f', 'q', 'p', 'ap', 'X', 'l', 'z', 'T']]
                    msg_values.append(f"{usd_size:.2f}")
                    with open(filename, 'a') as f:
                        trade_info = ','.join(msg_values) + '\n'
                        trade_info = trade_info.replace('USDT', '')
                        f.write(trade_info)

            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(5)

asyncio.run(binance_liquidation(websocket_url, filename))



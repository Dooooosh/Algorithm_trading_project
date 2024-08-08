import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect, ConnectionClosed
from termcolor import cprint

#Configuration variables
websocket_url = 'wss://fstream.binance.com/ws/!forceOrder@arr'
output_dir = '/Users/Ohm/My_Algoes/data-streams/csv_files'
filename = os.path.join(output_dir, 'binance_liquidations.csv')
DEBUG = False #Set to False for production

# Check if the output directory exists, if not, create it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

#Check if the CSV file already exists
if not os.path.isfile(filename):
    #If not, create it and write the header row
    with open(filename, 'w') as f:
        f.write(",".join([
            'symbol', 'side', 'order_type', 'time_in_force',
            'original_quantity', 'price', 'average_price', 'order_status',
            'order_last_filled_quantity', 'order_filled_accumulated_quantity', 
            'order_trade_time', "usd_size"
        ])+ "\n")

async def process_message(message, filename):
    try:
        order_data = json.loads(message)['o']
        #Remove 'USDT' from the symbol for display and logging
        symbol = order_data['s'].replace('USDT', '')
        side = order_data['S']
        timestamp = int(order_data['T'])
        filled_quantity = float(order_data['z'])
        price = float(order_data['p'])
        usd_size = filled_quantity * price
                
        bkk_tz = pytz.timezone('Asia/Bangkok')
        time_bkk_tz = datetime.fromtimestamp(timestamp/1000, bkk_tz).strftime('%H:%M:%S')
                
        if usd_size > 3000:
            liquidation_type = 'L LIQ' if side == 'SELL' else "S LIQ"
            # make symbol only first 4 letters
            symbol = symbol[:4]

            output = f"{liquidation_type} {symbol} {time_bkk_tz} ${usd_size:,.0f}"
            color = 'green' if side == 'SELL' else 'red'
            attrs = ['bold'] if usd_size > 10000 else []

            if usd_size > 500000:  # Very large liquidations
                stars = '*' * 3
                attrs.append('blink')
                output = f'{stars}{output}'
                for _ in range(4):
                    cprint(output, 'white', f'on_{color}', attrs=attrs)
            elif usd_size > 250000:  # Large liquidations
                stars = '*' * 2
                attrs.append('blink')
                output = f'{stars}{output}'
                for _ in range(3):
                    cprint(output, 'white', f'on_{color}', attrs=attrs)
            elif usd_size > 100000:  # Significant liquidations
                stars = '*' * 1
                attrs.append('blink')
                output = f'{stars}{output}'
                for _ in range(2):
                    cprint(output, 'white', f'on_{color}', attrs=attrs)
            elif usd_size > 25000:  # Noteworthy liquidations
                cprint(output, 'white', f'on_{color}', attrs=attrs)
            else:
                cprint(output, 'white', f'on_{color}')

            print('')


        #Append USD size to msg_values and write to CSV
        msg_values = [str(order_data.get(key)) for key in ['s', 'S', 'o', 'f', 'q', 'p', 'ap', 'X', 'l', 'z', 'T']]
        msg_values.append(str(usd_size))
        with open(filename, 'a') as f:
            trade_info = ','.join(msg_values) + '\n'
            #Replace 'USDT' in the symbol for CSV logging as well
            trade_info = trade_info.replace('USDT', '')
            f.write(trade_info)

    except Exception as e:
        print(f'Error processing message: {e}')
        
async def simulate_websocket_break(websocket):
    await asyncio.sleep(30)
    await websocket.close()
    print('DEBUG: Simulated websocket break')
    
async def connect_and_consume(uri, filename):
    while True:
        try:
            async with connect(uri) as websocket:
                print(f'Connected to {uri}')
                if DEBUG:
                    asyncio.create_task(simulate_websocket_break(websocket))
                while True:
                    try:
                        msg = await websocket.recv()
                        await process_message(msg, filename)
                    except ConnectionClosed:
                        print('Connection closed, reconnecting...')
                        break
                    except Exception as e:
                        print(f'Error receiving message: {e}')
        except Exception as e:
            print(f'Failed to connect: {e}')
        await asyncio.sleep(5) #Wait before attemping to reconnect
        
#Run the event loop
asyncio.run(connect_and_consume(websocket_url, filename))

                        
            
    




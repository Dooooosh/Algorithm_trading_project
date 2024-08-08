import asyncio
import json
from datetime import datetime
from websockets import connect, ConnectionClosed
from termcolor import cprint
import pandas as pd
import os

# Symbols for which you want to stream funding rates
symbols = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'aaveusdt', 'avaxusdt', 'dogeusdt', 'linkusdt', 'trxusdt', 'xrpusdt']
websocket_url_base = 'wss://fstream.binance.com/ws/'
output_dir = '/Users/Ohm/My_Algoes/data-streams/csv_files'
filename = os.path.join(output_dir, 'funding_rates.csv')

#Shared variables across coroutines to keep thesymbol count
#Using a dictionary to keep it mutable at the coroutine level
shared_symbol_counter = {'count': 0}
print_lock = asyncio.Lock() #Lock to manage orderly console output

#List to store funding data
funding_data = []

# Check if the output directory exists, if not, create it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

async def binance_funding_stream(symbol, shared_counter):
    global print_lock, funding_data
    websocket_url = f'{websocket_url_base}{symbol}@markPrice'
    while True:
        try:
            async with connect(websocket_url) as websocket:
                while True:
                    try:
                        
                        # Get a lock before accessing the shared counter or printing to the console
                        async with print_lock:
                            message = await websocket.recv()
                            data = json.loads(message)
                            event_time = datetime.fromtimestamp(data['E'] / 1000).strftime('%H:%M:%S')
                            symbol_display = data['s'].replace('USDT', '')
                            funding_rate = float(data['r'])
                            yearly_funding_rate = (funding_rate * 3 * 365) * 100

                            if yearly_funding_rate > 50:
                                text_color, back_color = 'black', 'on_red'
                            elif yearly_funding_rate > 30:
                                text_color, back_color = 'black', 'on_yellow'
                            elif yearly_funding_rate > 5:
                                text_color, back_color = 'black', 'on_cyan'
                            elif yearly_funding_rate < -10:
                                text_color, back_color = 'black', 'on_green'
                            else:
                                text_color, back_color = 'black', 'on_lightgreen'

                            cprint(f"{symbol_display} funding: {yearly_funding_rate:.2f}%", text_color, back_color)
                            
                            # Increment the encounter each time a symbol is printed
                            shared_counter['count'] += 1
                            
                            #If all symbols have been printed, insert a line break
                            if shared_counter['count'] >= len(symbol):
                                cprint(f"{event_time} Yrly fund", 'white', 'on_black')
                                shared_counter['count'] = 0 #Reset counter
                                
                            # Add data to the funding_data list
                            funding_data.append({
                                'symbol': symbol_display,
                                'event_time': event_time,
                                'funding_rate': yearly_funding_rate,
                                'yearly_funding_rate': yearly_funding_rate
                            })
                            
                            #Periodically save to CSV
                            if len(funding_data) >= len(symbols):
                                df = pd.DataFrame(funding_data)
                                df.to_csv(filename, index=False)
                                funding_data = [] #Clear the list after saving
                                
                    except ConnectionClosed:
                        print(f'Connection closed for {symbol}, reconnecting...')
                        break
                    except Exception as e:
                        #print(f'An exception occured in stream {symbol}: {e}')
                        await asyncio.sleep(5)
                        
        except Exception as e:
            print(f'Failed to connect for {symbol}: {e}')
        await asyncio.sleep(5) #Wait before attemping to reconnect


async def main():
    #Create a task for each symbol
    tasks = [binance_funding_stream(symbol, shared_symbol_counter) for symbol in symbols]
    await asyncio.gather(*tasks)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())


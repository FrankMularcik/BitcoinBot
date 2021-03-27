import os, json, websocket, dateutil.parser, pandas as pd, btalib, cbpro, requests, gspread
from datetime import datetime
from datetime import timedelta
import time
from oauth2client.service_account import ServiceAccountCredentials
from keep_alive import keep_alive

#create a json file to log some of the data for the script so if it starts over it still works

minutes_processed = {}
candlesticks = []
current_mess = None
previous_mess = None
df = pd.DataFrame(columns = ["Open", "High", "Low", "Close", "sma_s", "sma_l", "cross"])
coin_client = None
sheet = None


def open_json(text_file):
  with open(text_file) as infile:
    new_json = json.load(infile)
    infile.close()
  return new_json

def to_json(dictionary, text_file):
  with open(text_file, 'w') as outfile:
    outfile.write(json.dumps(dictionary))
    outfile.close()

def document(cb, side, spreadsheet, json_file=None):  
  order = cb.place_market_order(product_id='BTC-USD', side=side, size=0.5)
  time.sleep(0.1)
  #transact_row = int(json_file['transact_row'])
  transact_row = sheet.cell(3, 20).value
  spreadsheet.update_cell(transact_row, 8, datetime.now().strftime("%m/%d/%y %H:%M"))
  spreadsheet.update_cell(transact_row, 9, side)
  spreadsheet.update_cell(transact_row, 10, cb.get_order(order['id'])['executed_value'])
  spreadsheet.update_cell(transact_row, 11, cb.get_order(order['id'])['fill_fees'])
  #transact_row = transact_row + 1
  #json_file['transact_row'] = str(transact_row)

  if side == 'buy':
    in_position = 'TRUE'
  else:
    in_position = 'FALSE'
  
  spreadsheet.update_cell(5, 20, in_position)

def on_open(ws):
    global coin_client, sheet
    print("Connected")

    scope = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    try:
      creds =  ServiceAccountCredentials.from_json_keyfile_name('Sheets API Key.env', scope)
    except:
      print("error here")
    client = gspread.authorize(creds)

    sheet = client.open('Bitcoin').worksheet("50-100 sma")

    url = "https://api-public.sandbox.pro.coinbase.com"

    coin_client = cbpro.AuthenticatedClient(
        os.getenv("API"),
        os.getenv("API_SECRET"),
        os.getenv("PASSPHRASE"),
        api_url = url
    )

    send_data = {
        "type": "subscribe", 
        "channels": [
            {

            "name": "ticker",
            "product_ids": ["BTC-USD"]
            }
        ]
    }


    ws.send(json.dumps(send_data))

def on_close(ws):
    print("Closed")

def on_message(ws, message):
    global current_mess, previous_mess, df, minutes_processed, candlesticks, sheet
    
    previous_mess = current_mess
    current_mess = json.loads(message)
    
    if current_mess['type'] != 'ticker':
      return

    time_as_datetime = dateutil.parser.isoparse(current_mess["time"]).strftime("%m/%d/%y %H:%M")
    times = time_as_datetime
    
    if not times in minutes_processed:
        print("---New Minute---")
        print("{} @ {}".format(current_mess["price"], times))
        minutes_processed[times] = True
        #print(candlesticks[-1])
        #print(minutes_processed)
        
        
        #print(candlesticks)

        if len(candlesticks) > 0:
            #print(candlesticks[-1])
            candlestick = candlesticks[-1]
            candlestick["Close"] = previous_mess["price"]

            #json_stats = open_json("stats.txt")
            
            # minute_row = int(json_stats['minute_row'])
            # in_position = json_stats['in_position']
            minute_row = sheet.cell(1, 20).value
            in_position = sheet.cell(5, 20).value
            
            sheet.update_cell(minute_row, 1, candlestick["time"])
            sheet.update_cell(minute_row, 2, candlestick["Close"])
            sheet.update_cell(minute_row, 3, df['sma_s'][-1])
            sheet.update_cell(minute_row, 4, df['sma_l'][-1])
            sheet.update_cell(minute_row, 5, df['cross'][-1])
            sheet.update_cell(minute_row, 6, in_position)
            #minute_row = minute_row + 1
            #json_stats['minute_row'] = str(minute_row)
            data = []
            keys = list(candlestick.keys())[1:]
          
            for key in keys:
                data.append(candlestick[key])
            data.append(0)
            data.append(0)
            data.append(0)
            time = candlestick["time"]
            #candlestick.pop("time")
            #print(candlestick)
            dc = pd.DataFrame([data], columns = list(df.columns), index = [time])

            #print(dc)
            df = pd.concat([df, dc])

            #print(df.round(1))

            #print("SMA")
            #time = candlesticks[-1]['time']
            try:
                #print("done")
                sma_short = btalib.sma(df.Close, period = 50)
                sma_long = btalib.sma(df.Close, period = 100)
                
                df['sma_s'] = sma_short.df
                df['sma_l'] = sma_long.df
                
                
            except:
                print("Error")

            if float(df['sma_l'].iloc[-2]) > float(df['sma_s'].iloc[-2]):
                    if float(df['sma_l'].iloc[-1]) < float(df['sma_s'].iloc[-1]):
                        df.loc[candlestick["time"], 'cross'] = 1
                        if in_position == 'FALSE':
                          #json_stats = document(coin_client, 'buy', sheet, json_stats)
                          document(coin_client, 'buy', sheet)
            elif float(df['sma_l'].iloc[-2]) < float(df['sma_s'].iloc[-2]):
                    if float(df['sma_l'].iloc[-1]) > float(df['sma_s'].iloc[-1]):
                        df.loc[candlestick["time"], 'cross'] = -1
                        if in_position == 'TRUE':
                          #json_stats = document(coin_client, 'sell', sheet, json_stats)       
                          document(coin_client, 'sell', sheet) 
            
            #to_json(json_stats, "stats.txt")

            if len(candlesticks) > 10:
                candlesticks = candlesticks[-10:]

            key_times = list(minutes_processed.keys())
            if len(key_times) > 10:
              new_minutes = {}
              
              for key in key_times[-10:]:
                new_minutes[key] = minutes_processed[key]
              minutes_processed = new_minutes  

        candlesticks.append({
            "time": times,
            "Open": current_mess["price"],
            "High": current_mess["price"],
            "Low": current_mess["price"],
            "Close": current_mess["price"]
        })

        if len(df) > 105:
            df = df[-105:]

    

        
    if len(candlesticks) > 0:
        if current_mess["price"] > candlesticks[-1]["High"]:
            candlesticks[-1]["High"] = current_mess["price"]
        if current_mess["price"] < candlesticks[-1]["Low"]:
            candlesticks[-1]["Low"] = current_mess["price"]
    


end_time = datetime.utcnow().isoformat()
start_time = (datetime.utcnow() - timedelta(minutes=100)).isoformat()
r = requests.get("https://api-public.sandbox.pro.coinbase.com/products/BTC-USD/candles?start={}&end={}&granularity={}".format(start_time, end_time, 60))
candles = json.loads(r.content)

for i in range(1,len(candles)+1):
    candle = candles[-i]
    ind = datetime.fromtimestamp(candle[0]).strftime("%m/%d/%y %H:%M")
    dc = pd.DataFrame([[candle[3], candle[2], candle[1], candle[4], 0, 0, 0]], columns = list(df.columns), index = [ind])
    df = pd.concat([df, dc])


#print(df)

keep_alive()

stream = "wss://ws-feed.pro.coinbase.com"

s = websocket.WebSocketApp(stream, on_open = on_open, on_close=on_close, on_message=on_message)
s.run_forever()
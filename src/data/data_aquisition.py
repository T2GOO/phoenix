import ccxt
import pandas as pd
import time
import plotly.graph_objects as go
import multiprocessing
import os
import pandas_ta as ta

def display_ohlc(csv_file):
    df = pd.read_csv(csv_file)
    df = df.tail(min(len(df), 50_000))

    fig = go.Figure(data=go.Ohlc(x=df['date'],
                        open=df['open'],
                        high=df['high'],
                        low=df['low'],
                        close=df['close']))
    fig.show()

def history_fetch(period, n):
    print(f"Start aquisition // history //- timescale: {period} - nb of data: {n}")
    symbol = 'BTC/USDT'
    try:
        binance = ccxt.binance({
            'enableRateLimit': True,
        })
        match period:
            case 'sec' :
                timeframe = '1s'
                file_name = os.path.join("data", "history", "ohlc_1s.csv")
            case 'min' :
                timeframe = '1m'
                file_name = os.path.join("data", "history", "ohlc_1m.csv")
        limit = min(n, 1000)
        all_data = []
        pd.DataFrame([[]]).to_csv(file_name, index=False, header = False, mode='w')

        timeframe_seconds = binance.parse_timeframe(timeframe) * 1000  # Convert timeframe to milliseconds
        # Calculate the start time for the first request
        since = int(time.time() * 1000) - (n * timeframe_seconds)
        from datetime import datetime
        print("Data from: ", datetime.utcfromtimestamp(since/1000).strftime('%Y-%m-%d %H:%M:%S')) 
        while len(all_data) < n:
            print(f"- Aquisition // history // ({period}) {len(all_data)} / {n}")
            remaining = n - len(all_data)  # Calculate how many more data points are needed
            fetch_limit = min(limit, remaining)  # Fetch only as much as needed
            data = binance.fetch_ohlcv(symbol, timeframe, since, fetch_limit)
            if not data:
                break  # Stop if no more data is returned
            all_data.extend(data)
            since = data[-1][0] + timeframe_seconds
            time.sleep(binance.rateLimit / 1000)
            df = pd.DataFrame(data, columns=['date','open','high','low','close','volume'])
            save_data_csv(df, file_name, 'a')
    except Exception as e:
        print(f"Error fetching data: {e}")
    print(f"Aquisition // history // ({period}) ended")

def real_time_fetch(period, duration):
    print(f"Start aquisition // real time // - timescale: {period} - duration: {duration} sec")
    binance = ccxt.binance({
        'enableRateLimit': True,
    })
    symbol = 'BTC/USDT'
    match period:
        case 'sec' :
            timeframe = '1s'
            limit = 60
            sleep_time = 60
            file_name = os.path.join("data", "rt", "ohlc_1s.csv")
        case 'min' :
            timeframe = '1m'
            limit = 60
            sleep_time = 3600
            file_name = os.path.join("data", "rt", "ohlc_1m.csv")
    since = None
    start_time = time.time()
    count = 0
    pd.DataFrame([[]]).to_csv(file_name, index=False, header = False, mode='w')
    while while_condition(duration, start_time):
        ohlcv = binance.fetch_ohlcv(symbol, timeframe, since, limit)
        if not ohlcv:
            continue
        df = pd.DataFrame(ohlcv, columns=['date','open','high','low','close','volume'])
        since = ohlcv[-1][0] + 1
        save_data_csv(df, file_name, 'a')
        count += 1
        print(f"- Aquisition // real time // ({period}) {count}")
        time.sleep(sleep_time)
    print(f"Aquisition // real time // ({period}) ended")

def while_condition(condition, start_time):
    if condition < 0:
        return True
    elif start_time + condition < time.time():
        return False
    return True

def save_data_csv(df, file_name, mode = "w"):
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    df.sort_values('date', inplace=True)
    df.to_csv(file_name, index=False, header = False, mode=mode)

def deamon_aquisition(period = 'min', duration = -1, daemon = False):
    process = multiprocessing.Process(target=real_time_fetch, args=(period,duration,))
    process.daemon = daemon
    process.start()
    return process

def add_indicators(df):

    df['rsi'] = ta.rsi(df['close'], length=14) 

    macd = ta.macd(df['close'])
    df['macd'] = macd['MACD_12_26_9']
    df['macd_signal'] = macd['MACDs_12_26_9']
    df['macd_hist'] = macd['MACDh_12_26_9']

    df['sma_50'] = ta.sma(df['close'], length=50) 

    boll = ta.bbands(df['close'], length=20)
    df['bbl'] = boll['BBL_20_2.0']
    df['bbm'] = boll['BBM_20_2.0']
    df['bbu'] = boll['BBU_20_2.0']
    df['bbb'] = boll['BBB_20_2.0']
    df['bbp'] = boll['BBP_20_2.0']

    df = df.tail(len(df)-50) # remove rows with empty data
    return df

def update_buffers(buf_type):
    # for 1s
    try:
        df = pd.read_csv(os.path.join("data", buf_type, "ohlc_1s.csv"), names=['date','open','high','low','close','volume'])
        df = add_indicators(df)
        df.to_csv(os.path.join("data", "ohlc_and_indicators_1s.csv"), index=False, header = False, mode='a')
    except Exception as e:
        print("Error (1s) -> ", e)
    # for 1m
    try:
        df = pd.read_csv(os.path.join("data", buf_type,"ohlc_1m.csv"), names=['date','open','high','low','close','volume'])
        df = add_indicators(df)
        df.to_csv(os.path.join("data", "ohlc_and_indicators_1m.csv"), index=False, header = False, mode='a')
    except Exception as e:
        print("Error (1m) -> ", e)



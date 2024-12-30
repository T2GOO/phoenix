import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler

def preprocess_data():
    print('-- Start preprocess_data')
    data = pd.read_csv(os.path.join('data', 'ohlc_and_indicators_1s.csv')).tail(50_000)
    data.fillna(method='ffill', inplace=True)
    scaler = MinMaxScaler()
    features = ['volume', 'rsi', 'macd', 'macd_signal', 'macd_hist', 'sma_50', 'bbl', 'bbm', 'bbu', 'bbb', 'bbp']
    data[features] = scaler.fit_transform(data[features])
    print('-- End preprocess_data')

preprocess_data()
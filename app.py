from flask import Flask, request, jsonify
from datetime import datetime
import pandas as pd
from nsepython import equity_history
from nsepy import get_history
import yfinance as yf
from io import BytesIO
import os
from flask_cors import CORS
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
# Function to fetch NSE Spot data
def get_nse_spot(ticker, start_date, end_date):
    start_str = start_date.strftime("%d-%m-%Y")
    end_str = end_date.strftime("%d-%m-%Y")
    data = equity_history(ticker, "EQ", start_str, end_str)
    data.rename(columns={
        'CH_TIMESTAMP': 'Date',
        'CH_OPENING_PRICE': 'Open',
        'CH_TRADE_HIGH_PRICE': 'High',
        'CH_TRADE_LOW_PRICE': 'Low',
        'CH_CLOSING_PRICE': 'Close',
        'CH_TOT_TRADED_QTY': 'Volume',
    }, inplace=True)
    return data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

# Function to fetch NSE Futures data
def get_nse_futures(ticker, start_date, end_date, expiry_date):
    data = get_history(symbol=ticker, start=start_date, end=end_date, 
                     index=True, futures=True, expiry_date=expiry_date)
    data.rename(columns={
        'CH_TIMESTAMP': 'Date',
        'CH_OPENING_PRICE': 'Open',
        'CH_TRADE_HIGH_PRICE': 'High',
        'CH_TRADE_LOW_PRICE': 'Low',
        'CH_CLOSING_PRICE': 'Close',
        'CH_TOT_TRADED_QTY': 'Volume',
        'OPEN_INT': 'Open Interest'
    }, inplace=True)
    return data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']]

# Function to fetch MCX data
def fetch_mcx_data(symbol, start_date, end_date, frequency='1d'):
    data = yf.download(symbol, start=start_date, end=end_date, interval=frequency)
    data.reset_index(inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")
    data = data.resample('W-Mon', on='Date').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
    }).reset_index()
    data['Date'] = data['Date'].dt.strftime('%d-%m-%Y')
    return data

@app.route('/get-data/', methods=['POST'])
def get_data():
    data_request = request.get_json()
    ticker = data_request['ticker']
    start_date = datetime.strptime(data_request['start_date'], "%d-%m-%Y")
    end_date = datetime.strptime(data_request['end_date'], "%d-%m-%Y")
    expiry_date = data_request.get('expiry_date', None)

    if data_request['exchange'] == 'NSE':
        if '-FUT' in ticker:
            data = get_nse_futures(ticker.replace('-FUT', ''), start_date, end_date, expiry_date)
        else:
            data = get_nse_spot(ticker, start_date, end_date)
    elif data_request['exchange'] == 'MCX':
        data = fetch_mcx_data(ticker, start_date, end_date)

    data_dict = data.to_dict(orient="records")
    return jsonify({"data": data_dict})

if __name__ == '__main__':
    app.run(debug=True)

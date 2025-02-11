import logging
from flask import Flask, request, jsonify
from datetime import datetime
import pandas as pd
from nsepython import equity_history
from nsepy import get_history
import yfinance as yf
from io import BytesIO
import os
from flask_cors import CORS
from dateutil import parser

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Function to fetch NSE Spot data
def get_nse_spot(ticker, start_date, end_date):
    logging.info(f"Fetching NSE Spot data for {ticker} from {start_date} to {end_date}")
    start_str = start_date.strftime("%d-%m-%Y")
    end_str = end_date.strftime("%d-%m-%Y")
    
    try:
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
    except Exception as e:
        logging.error(f"Error fetching NSE Spot data: {e}")
        raise

# Function to fetch NSE Futures data
def get_nse_futures(ticker, start_date, end_date, expiry_date):
    logging.info(f"Fetching NSE Futures data for {ticker} from {start_date} to {end_date} with expiry {expiry_date}")
    
    try:
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
    except Exception as e:
        logging.error(f"Error fetching NSE Futures data: {e}")
        raise

# Function to fetch MCX data
def fetch_mcx_data(symbol, start_date, end_date, frequency='1d'):
    # Fetch data
    data = yf.download(symbol, start=start_date, end=end_date, interval=frequency)
    if isinstance(data.columns, pd.MultiIndex):
        # Flatten the MultiIndex columns (select the 'GOLD' level from the MultiIndex)
        data.columns = data.columns.get_level_values(0)  # Use the first level of MultiIndex
        logging.info("Flattened columns:", data.columns)
    
    # Check if data is fetched correctly
    logging.info("Columns in the fetched data:", data.columns)
    logging.info("First few rows of the fetched data:")
    logging.info(data.head())
    
    if data.empty:
        raise ValueError(f"No data fetched for symbol: {symbol}. Please check the symbol or date range.")
    
    # Reset index so that Date is a column and not an index
    data.reset_index(inplace=True)

    # Ensure Date is in datetime format
    data['Date'] = pd.to_datetime(data['Date'])
    
    # Check if the required columns are present
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Resample to weekly data (Monday as the start of the week)
    data = data.resample('W-Mon', on='Date').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        # Adding Open Interest if it exists
        **({'Open Interest': 'last'} if 'Open Interest' in data.columns else {})
    }).reset_index()

    # Format Date to 'DD-MM-YYYY'
    data['Date'] = data['Date'].dt.strftime('%d-%m-%Y')

    return data

@app.route('/get-data/', methods=['POST'])
def get_data():
    data_request = request.get_json()
    logging.info(f"Received request: {data_request}")

    try:
        ticker = data_request['ticker']
        start_date = parser.parse(data_request['start_date'])
        end_date = parser.parse(data_request['end_date'])
        expiry_date = data_request.get('expiry_date', None)
        if expiry_date:
            expiry_date = parser.parse(expiry_date)
        if data_request['exchange'] == 'NSE':
            if '-FUT' in ticker:
                data = get_nse_futures(ticker.replace('-FUT', ''), start_date, end_date, expiry_date)
            else:
                data = get_nse_spot(ticker, start_date, end_date)
        elif data_request['exchange'] == 'MCX':
            data = fetch_mcx_data(ticker, start_date, end_date)
        else:
            logging.warning(f"Unsupported exchange: {data_request['exchange']}")
            return jsonify({"error": "Unsupported exchange"}), 400

        data_dict = data.to_dict(orient="records")
        logging.info(f"Returning {len(data_dict)} records")
        return jsonify({"data": data_dict})
    
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("Starting Flask server...")
    app.run(debug=True)

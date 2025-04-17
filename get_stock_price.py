import yfinance as yf
from datetime import datetime, timedelta
import json
import sys
import csv
import time
import pandas as pd

def get_stock_data_for_db(tickers_list, year, batch_size=50):
    """
    Get close, high, and low prices for a list of tickers for a specific year,
    and return a DataFrame formatted for database loading.
    
    Parameters:
    tickers_list (list): List of ticker symbols
    year (int): Year for which to fetch data
    batch_size (int): Number of tickers to fetch in each batch
    
    Returns:
    DataFrame: Unpivoted DataFrame with columns ['ticker', 'date', 'high', 'low', 'close']
    """
    # Set the start and end dates for the year
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # Create an empty list to store each ticker's data
    all_stock_data = []
    
    # Process tickers in batches
    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i:i+batch_size]
        batch_tickers = " ".join(batch)
        
        try:
            print(f"Fetching batch {i//batch_size + 1}/{(len(tickers_list) + batch_size - 1)//batch_size}...")
            # Download data for the batch
            data = yf.download(batch_tickers, start=start_date, end=end_date, group_by='ticker')
            
            # Process each ticker in the batch
            for ticker in batch:
                if len(batch) == 1:
                    # If there's only one ticker in the batch
                    ticker_data = data[['Close', 'High', 'Low']] if not data.empty else pd.DataFrame()
                else:
                    # Multiple tickers
                    if ticker in data:
                        ticker_data = data[ticker][['Close', 'High', 'Low']] if not data[ticker].empty else pd.DataFrame()
                    else:
                        ticker_data = pd.DataFrame()
                
                if not ticker_data.empty:
                    # Reset index to make date a column
                    ticker_df = ticker_data.reset_index()
                    # Add ticker column
                    ticker_df['ticker'] = ticker
                    # Rename columns to lowercase
                    ticker_df.rename(columns={
                        'Date': 'date',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close'
                    }, inplace=True)
                    # Reorder columns to desired format
                    ticker_df = ticker_df[['ticker', 'date', 'high', 'low', 'close']]
                    # Append to our list
                    all_stock_data.append(ticker_df)
                else:
                    print(f"No data available for {ticker} in {year}")
                    
        except Exception as e:
            print(f"Error fetching batch {i//batch_size + 1}: {e}")
            # If a batch fails, try individual tickers
            for ticker in batch:
                try:
                    single_data = yf.download(ticker, start=start_date, end=end_date)
                    if not single_data.empty:
                        ticker_df = single_data[['Close', 'High', 'Low']].reset_index()
                        ticker_df['ticker'] = ticker
                        ticker_df.rename(columns={
                            'Date': 'date',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close'
                        }, inplace=True)
                        ticker_df = ticker_df[['ticker', 'date', 'high', 'low', 'close']]
                        all_stock_data.append(ticker_df)
                    else:
                        print(f"No data available for {ticker} in {year}")
                except Exception as e2:
                    print(f"Error fetching data for {ticker}: {e2}")
        
        # Add a small delay between batches to avoid rate limiting
        time.sleep(1)
    
    # Combine all data into a single DataFrame
    if all_stock_data:
        result_df = pd.concat(all_stock_data, ignore_index=True)
        result_df = result_df.reset_index(drop=True)
        return result_df
    else:
        return pd.DataFrame(columns=['ticker', 'date', 'high', 'low', 'close'])


if __name__ == "__main__":
    ticker_list_json = sys.argv[1]  # Read from command-line args
    ticker_data = json.loads(ticker_list_json)
    ticker_list = [item["ticker"] for item in ticker_data]
    year = sys.argv[2]
    stock_data = get_stock_data_for_db(ticker_list, year)
    file_path = 'stock_data.csv'
    stock_data.to_csv(file_path, index=False)
    
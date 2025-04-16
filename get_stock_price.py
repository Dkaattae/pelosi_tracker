import yfinance as yf
from datetime import datetime, timedelta
import json
import sys
import csv

def get_stock_price(txn):
	ticker = txn['ticker']
	date_str = txn['date']
	txn_type = txn['type']
	date = datetime.strptime(date_str, '%Y-%m-%d')
	start_date = (date - timedelta(days=2)).strftime('%Y-%m-%d')
	end_date = (date + timedelta(days=2)).strftime('%Y-%m-%d')

	# Get historical data from yfinance
	data = yf.download(ticker, start=start_date, end=end_date)
    
	if data.empty:
		print(f"No data found for {ticker} around {date_str}")

	closing_price = data.loc[date_str]['Close'].values[0].item()
	high_price = data.loc[date_str]['High'].values[0].item()
	low_price = data.loc[date_str]['Low'].values[0].item()
	# stock_value = closing_price * trading_shares
	# if txn_type.lower() == 's':
	# 	stock_value *= -1
	# cash_value = -stock_value

	result = {
		'ticker': ticker,
		'date': date_str,
		'type': txn_type,
		'closing_price': round(closing_price, 2),
		'high_price': round(high_price, 2),
		'low_price': round(low_price,2)
	}
	return result


if __name__ == "__main__":
    input_file = sys.argv[1]  # Read from command-line args
    stock_data = []
    error_log = []
    with open(input_file, 'r') as stock_lists:
        csv_reader = csv.DictReader(stock_lists)
        for txn in csv_reader:
            try:
                stock_data.append(get_stock_price(txn))
            except Exception as e:
                error_log.append({"ticker": txn['ticker'], "date": txn['date'], "error": e})
    file_path = 'stock_data.csv'
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(stock_data)
    with open('stock_data_error_log.csv', 'w', newline = '') as efile:
    	writer = csv.write(efile)
    	writer.writerow(error_log)
    
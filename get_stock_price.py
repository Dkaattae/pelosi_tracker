import yfinance as yf
from datetime import datetime, timedelta
import json

def get_stock_price(txn):
	ticker = txn['ticker']
	date_str = txn['date']
	txn_type = txn['type']
	shares = txn['shares']
	date = datetime.strptime(date_str, '%Y-%m-%d')
	start_date = (date - timedelta(days=2)).strftime('%Y-%m-%d')
	end_date = (date + timedelta(days=2)).strftime('%Y-%m-%d')

	# Get historical data from yfinance
	data = yf.download(ticker, start=start_date, end=end_date)
    
	if data.empty:
		print(f"No data found for {ticker} around {date_str}")

	closing_price = data.loc[date_str]['Close'].values[0].item()
	stock_value = closing_price * shares
	if txn_type.lower() == 's':
		stock_value *= -1
	cash_value = -stock_value

	result = {
		'ticker': ticker,
		'date': date_str,
		'type': txn_type,
		'shares': shares,
		'closing_price': round(closing_price, 2),
		'stock_value': round(stock_value, 2),
		'cash_value': round(cash_value, 2)
	}
	return result





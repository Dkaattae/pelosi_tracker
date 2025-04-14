import dlt
import requests
import sys
from io import StringIO
import json
from typing import List, Dict

api_key = sys.argv[1]
input_data = sys.argv[2]  # Read from command-line args
input_contracts = json.loads(input_data)


@dlt.resource(write_disposition="append", primary_key="contractID")
def process_contracts(contracts: List[Dict]):
    """
    Process contracts from JSON input and fetch last prices
    """
    
    for contract in contracts:
        try:
            # Fetch data from Alpha Vantage
            url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={contract['symbol']}&date={contract['date']}&apikey={api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Find matching contract
            strike_str = f"{contract['strike']:.2f}"
            for api_contract in data.get('contracts', []):
                if (
                    api_contract.get('type') == contract['type'] and
                    api_contract.get('strike') == strike_str and
                    api_contract.get('expiration') == contract['expiration']
                ):
                    yield {
                        'contractID': api_contract['contractID'],
                        'symbol': api_contract['symbol'],
                        'last_price': float(api_contract['last']),
                        'timestamp': dlt.sources.current.replace(microsecond=0)
                    }
                    break
            else:
                print(f"No match found for {contract}")
                
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for {contract['symbol']}: {str(e)}")
            continue

pipeline = dlt.pipeline(
    pipeline_name="options_prices",
    destination="postgres",
    dataset_name="options_market_data"
)

# Load data to PostgreSQL
load_info = pipeline.run(
    process_contracts(input_contracts).add_map(lambda x: x)  # Flatten structure
)

print(load_info)

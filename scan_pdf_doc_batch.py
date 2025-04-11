import requests
import pdfplumber
import pandas as pd
from math import floor
import sys
from kestra import Kestra
from io import StringIO
import json


def download_reporting_pdf(year, file_number):
	url = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{file_number}"

	response = requests.get(url)
	if response.status_code == 200:
		with open(file_number, "wb") as f:
			f.write(response.content)
		print(f"Downloaded {file_number}")
	else:
		print("File not found or access denied.")

def get_column_location(pdf_path):
	column_names = ['ID', 'Owner', 'Asset', 'Transaction', 'Date', 'Notification', 'Amount', 'Gains']
	words_location = []
	with pdfplumber.open(pdf_path) as pdf:
		for page_num, page in enumerate(pdf.pages):
			# print(f"Page {page_num + 1}:")
			
			words = page.extract_words()
			for word in words:
				text = word["text"]
				if text in column_names:
					x0, y0, x1, y1 = word["x0"], word["top"], word["x1"], word["bottom"]
					words_location.append({'Text': text, 'X0': x0, 'Y0': y0, 'X1': x1, 'Y1': y1})
					# print(f"Text: {text}, X0: {x0}, Y0: {y0}, X1: {x1}, Y1: {y1}")
	last_col_X0 = 0
	column_location = []
	for col in words_location:
		if col['Text'] == 'ID' and col['X0'] > 50:
			continue
		if last_col_X0 >= col['X0']:
			continue
		column_location.append(col)
		last_col_X0 = col['X0']

	columns = [floor(col['X0']-1) for col in column_location]
	return columns


def extract_table_custom(pdf_path, columns):
    trade_events = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words()

            # Sort words by Y (top-to-bottom) first, then by X (left-to-right)
            words.sort(key=lambda w: (w["top"], w["x0"]))

            table = []
            row = [""] * len(columns)
            last_y = None  # Track the last Y position for row changes

            for word in words:
                # Start scanning only when word["top"] > 280
                if word["top"] <= 280 and page.page_number == 1:
                    continue  

                x, y = word["x0"], word["top"]

                # Find the correct column index for the word
                col_index = next((i for i, c in enumerate(columns) if x < c), len(columns) - 1)

                # If it's the first word being added, initialize the table properly
                if not table:
                    table.append(row)

                # If the word belongs to a new row (Y position changes significantly)
                if last_y is not None and y > last_y + 10:
                    table.append(row)
                    row = [""] * len(columns)

                # Assign the word to its column
                row[col_index] = row[col_index] + " " + word["text"] if row[col_index] else word["text"]
                last_y = y  # Update last Y position

            # Append the last row if it's not empty
            if row and any(row):
                table.append(row)

            # Print the formatted table
            for r in table:
                # print(r)
                trade_events.append(r)
    return trade_events

def combine_rows(pdf_list):
	trade_list = []
	for row in pdf_list:
		if row[1] == 'ID' or row[4] == 'Type' or row[7].find('?') != -1:
			continue
		if row[3] != '' and row[4] != '' and '\x00' not in row[3]:
			trade_event = {}
			trade_event['ID'] = row[1]
			trade_event['Owner'] = row[2]
			trade_event['Asset'] = row[3]
			trade_event['Transaction Type'] = row[4]
			trade_event['Date'] = row[5]
			trade_event['Notification Date'] = row[6]
			trade_event['Amount'] = row[7]
			trade_list.append(trade_event)
		if row[3].find('[') != -1 and row[3].find(']') != -1 and row[4] == '' and '\x00' not in row[3]:
			trade_event = trade_list[-1]
			trade_event['Asset'] += ' ' + row[3]
			trade_event['Amount'] += ' ' + row[7]
		if row[2] == '' and '\x00' in row[3] and row[3].startswith("F"):
			trade_event = trade_list[-1]
			trade_event['Filing Status'] = row[3].split(':')[1].replace('\x00', '')
		if row[2] == '' and '\x00' in row[3] and row[3].startswith('D'):
			trade_event = trade_list[-1]
			trade_event['Description'] = ''.join(row).replace('\x00', '')
	return trade_list


if __name__ == "__main__":
	input_data = sys.argv[1]  # Read from command-line args
	data = json.loads(input_data)
	trade_event_df = pd.DataFrame()
	for each_input in data:
		year = each_input['year']
		docid = each_input['docid']
		file_number = f'{docid}.pdf'
		download_reporting_pdf(year, file_number)
		columns = get_column_location(file_number)
		if columns == []:
			print(f'cannot scan file {file_number}')
			continue
		pdf_list = extract_table_custom(file_number, columns)
		trade_list = combine_rows(pdf_list)
		df = pd.DataFrame(trade_list)
		df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
		trade_df = df[~pd.isnull(df['Date'])]
		trade_df_rename = trade_df.rename(columns={'Transaction Type': 'Transaction_Type', 'Notification Date': 'Notification_Date', 'Filing Status': 'Filing_Status'})
		trade_df_rename['docid'] = docid
		trade_event_df = pd.concat([trade_event_df, trade_df_rename], ignore_index=True)
		# csv_str = trade_df.to_csv(index=False)
	trade_event_df.to_csv('trade_events.csv', index=False)
	outputs = {
		'trade_event_count': len(trade_event_df)
	}
	Kestra.outputs(outputs)

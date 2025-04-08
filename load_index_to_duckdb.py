import requests
import zipfile
import io
import duckdb
from datetime import datetime

# incremental load

current_year = datetime.now().year
index_url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{current_year}FD.zip"
response = requests.get(index_url)
response.raise_for_status()

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
	file_list = z.namelist()
	xml_filename = next((f for f in file_list if f.endswith(".xml")), None)
	if xml_filename:
		with z.open(xml_filename) as xml_file:
			xml_content = xml_file.read().decode("utf-8")  # Read and decode XML
			print("Extracted XML file") 
	else:
		print("No XML file found in ZIP.")

df_incremental = pd.read_xml(xml_content)


# Connect to DuckDB
con = duckdb.connect("houseclerk_transaction_index.db")

# Insert only new records based on a unique key (e.g., doc_id)
con.execute("""
    INSERT INTO transaction_doc_index
    SELECT * FROM df_incremental
    WHERE NOT EXISTS (
        SELECT 1 FROM transaction_doc_index t WHERE t.DocID = df_incremental.DocID
    )
""")


# Close connection
con.close()
## Description: 
Retail traders always saying Pelosi has insider information so that she made a lot of money.
Let’s track how house clerks trade   
This project is completely local. (because i do not have gcp credits anymore.)

## Overview:
<img width="977" alt="Screenshot 2025-04-10 at 9 37 44 PM" src="https://github.com/user-attachments/assets/b5477994-e434-4f8e-8b02-523a303a344b" />

1, download index file and load to docdata table. Index tables are partitioned by year, because the latest year is updating regularly, the rests are not.   
2, read DocID from index tables, requests doc file, scan doc file, load trading events into trading events table.    
Note: some files are delivered in person which do not look like those e-filled pdf. I ignored those for now.   
3, transform raw data from pdf to meaningful trading data if they are stock or options (asset type is ST or OP)   
4, using dlt to get stock/ option price and load to database   
5, transform trading events and prices to profits and counts   
6, dashboard showing profits vs benchmark   

## Tech stacks: 
1, kestra as orchestrator   
2, Postgres and pgadmin in docker as local database   
3, dlt to load options prices, python script to load stock prices   
4, dbt to transform data   
5, metabase for dashboard   

## Steps:
#### 1, kestra

`Docker-compose up -d `    

This command will spin up kestra, Postgres database, pgadmin and metabase.   
note: there are two other databases, for kestra internal use and metabase internal use.  

Run flows:    
1), run download_index_flow.yml first to load index into table docdata_<year>   
    initial load need to manually iterate over years you want to download.   
    the flow is scheduled first day of every month on 9am to download year 2025 index and load new data into table.   
    trigger could be changed to daily.   
2), import file scan_pdf_doc_batch.py into kestra   
3), run iterate_docpdf_batch.yml to scan pdf and initially load data into table houseclerk_trade_events      
    first time need to mannually iterate years wanted.   
    run iterate_docpdf_incremental.yml to scan new docs.   
    incremental loading is scheduled first day of every month on 10am to download new pdf doc and load into table.   
    trigger could be changed to daily.   

#### 2, dbt

* in development    
Pip install dbt[postgres] if not installed   
Then follow prompts to fill out info listed in docker compose yaml file.   
dbt init <project name>   
Then make changes to files in project   
dbt built   

* in production   
running kestra postgres_dbt.yaml

#### 3, dlt
* in development   
Pip install dlt[postgres]   
dlt init <project name>   
Then change secrets.toml with Postgres info listed in docker compose yaml   
Run load_options_prices.py   
* in production     
i setup a kv store, key is 'ALPHA_VANTAGE_API_KEY'.   
get your free api key here: https://www.alphavantage.co/support/#api-key   
limit 25 requests/day
import load_options_prices.py into kestra
run kestra get_option_price.yml in kestra
note: flow 'get_options_price.yaml' is running after dbt.    

#### 4, metabase    
go to localhost:3000    
then setup metabase connection by following prompt. information are in docker compose file   

Dashboard   

<img width="1026" alt="Screenshot 2025-04-13 at 11 14 14 PM" src="https://github.com/user-attachments/assets/6b38648f-3f4a-46df-960c-60ab79f2cf7a" />

<img width="1064" alt="Screenshot 2025-04-13 at 11 14 29 PM" src="https://github.com/user-attachments/assets/29e2a134-4d45-4b37-9e37-3c4cd42f8826" />

<img width="1064" alt="Screenshot 2025-04-13 at 11 14 39 PM" src="https://github.com/user-attachments/assets/91926c6e-f4a4-417c-b542-2d56cbef84cf" />

i only downloaded data in 2024 and 2025 for this dashboard, historical data is open to download.   
stock is most favorable for house clerk of representatives.    
Pelosi is not even the most aggresive trader in house. 

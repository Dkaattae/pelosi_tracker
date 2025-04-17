{{
    config(
        materialized='view'
    )
}}


select * 
from {{ source('staging', 'stock_data') }}

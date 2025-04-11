{{
    config(
        materialized='view'
    )
}}

select 
    prefix, last, first, 
    suffix, filingtype, statedst, 
    left(statedst, 2) as clerk_state, 
    year, filingdate, docid, updatedtime
from {{ source('staging', 'docdata_2025') }}
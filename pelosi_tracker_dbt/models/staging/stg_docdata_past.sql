{{
    config(
        materialized='view'
    )
}}

with past_docdata as (
    select *
    from {{ source('staging', 'docdata_2023') }}
    union all
    select *
    from {{ source('staging', 'docdata_2024') }}
)

select 
    prefix, last, first, 
    suffix, filingtype, statedst, 
    left(statedst, 2) as clerk_state, 
    year, filingdate, docid, updatedtime
from past_docdata
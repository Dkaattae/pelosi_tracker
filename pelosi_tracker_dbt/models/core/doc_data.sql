{{
    config(
        materialized='table'
    )
}}

with past_docdata as (
    select *
    from {{ ref('stg_docdata_past') }}
), 
current_docdata as (
    select *
    from {{ ref('stg_docdata_current_year')}}
)

select *
from past_docdata
union all
select *
from current_docdata
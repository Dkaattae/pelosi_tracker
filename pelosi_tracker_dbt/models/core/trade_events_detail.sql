{{ config(materialized='table') }}

select *
from {{ ref('stg_trade_events') }}
{{
    config(
        materialized='table'
    )
}}

with doc_data as (
    select clerk_state, statedst, docid
    from {{ ref('doc_data') }}
),

trading_events as (
    select docid, asset_type, transaction_type
    from {{ ref('stg_trade_events') }}
),

count_by_statedst as (
    select clerk_state, statedst, asset_type, count(*) as transaction_count
    from trading_events
    left outer join doc_data
        on trading_events.docid = doc_data.docid
    group by clerk_state, statedst, asset_type
)

select 
    asset_type,
    clerk_state,
    max(transaction_count) as max_count,
    avg(transaction_count) as avg_count,
    min(transaction_count) as min_count
from count_by_statedst
group by asset_type, clerk_state




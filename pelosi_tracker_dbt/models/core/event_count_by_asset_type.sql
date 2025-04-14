{{
    config(
        materialized='table'
    )
}}

with trading_events as (
    select *
    from {{ ref('stg_trade_events') }}
),
trading_events_by_asset_type as (
    select asset_type, count(*) as trading_count
    from trading_events
    group by asset_type
),
asset_type_lookup as (
    select *
    from {{ ref('asset_type_lookup') }}
)

select 
    trading_events_by_asset_type.asset_type,
    asset_type_lookup.asset_name,
    trading_events_by_asset_type.trading_count
from trading_events_by_asset_type
left outer join asset_type_lookup
    on trading_events_by_asset_type.asset_type = asset_type_lookup.asset_code
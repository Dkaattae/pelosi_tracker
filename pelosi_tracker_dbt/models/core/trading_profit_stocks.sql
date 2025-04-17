{{
    config(
        materialized='table'
    )
}}

with trading_stocks_value as (
    select *
    from {{ ref('trading_stocks_value') }}
),
stock_data as (
    select *
    from {{ ref('stock_data') }}
),
stock_last_trading_day as (
select 
	clerk_name,
	asset_ticker,
	max(date) as last_trading_day
from trading_stocks_value
group by clerk_name, asset_ticker
),
latest_trading_date as (
	select max(date) as latest_trading_day
	from stock_data
)
select 
	trading_stocks_value.*,
	stock_data.closing_price as latest_closing_price,
	stock_data.closing_price * est_current_shares as est_stock_current_value
from trading_stocks_value
inner join stock_last_trading_day
	on trading_stocks_value.clerk_name = stock_last_trading_day.clerk_name
	and trading_stocks_value.asset_ticker = stock_last_trading_day.asset_ticker
	and trading_stocks_value.date = stock_last_trading_day.last_trading_day
left outer join stock_data
	on trading_stocks_value.asset_ticker = stock_data.ticker
	and stock_data.date = (select latest_trading_day from latest_trading_date)
{{
    config(
        materialized='table'
    )
}}

with stock_trades as (
	select *
	from {{ ref('trade_events_detail') }}
	where asset_type = 'ST'
),

stock_data as (
	select *
	from {{ ref('stock_data') }}
),

stock_info as (
    select 
		concat(first, ' ', last) as clerk_name,
	    stock_trades.docid,
	    stock_trades.asset_ticker,
	    stock_trades.transaction_type,
	    stock_trades.date,
	    case
	    when number_of_shares is not null
	    then number_of_shares
	    else floor((amount_range_high+amount_range_low)/2/closing_price)
        end as est_shares,
        case
        when number_of_shares is not null
        then number_of_shares
        else floor(amount_range_low/high_price)
        end as est_shares_low,
        case
        when number_of_shares is not null
        then number_of_shares
        else floor(amount_range_high/low_price)
        end as est_shares_high,
        stock_trades.amount_range_low,
        stock_trades.amount_range_high,
        stock_trades.number_of_shares,
        stock_data.closing_price,
        stock_data.high_price,
        stock_data.low_price
    from stock_trades
    left outer join stock_data
        on stock_trades.asset_ticker = stock_data.ticker
        and stock_trades.date = stock_data.date
	left outer join doc_data
		on stock_trades.docid = doc_data.docid
	where asset_ticker is not null
),
events_row_number as (
select 
	row_number() over(partition by clerk_name, asset_ticker order by date) as ticker_row_num,
	case	
	when transaction_type like 'P'
	then est_shares
	when transaction_type like 'S%'
	then -est_shares
	end as qty,
	*
from stock_info
),
selling_order_row_number as (
select 
	row_number() over(partition by clerk_name, asset_ticker order by date) as selling_order_row_num,
	clerk_name,
	asset_ticker,
	ticker_row_num as selling_row_index
from events_row_number
where transaction_type = 'S'
),
previous_selling_order_number as (
select 
	selling_order_row_num,
	clerk_name,
	asset_ticker,
	selling_row_index,
	lag(selling_row_index, 1) over(partition by clerk_name, asset_ticker order by selling_row_index) as previous_selling_index
from selling_order_row_number
),
trading_block_number as (
select 
	events_row_number.*,
	coalesce(selling_order_row_num, 0) as selling_blocks
from events_row_number
left outer join previous_selling_order_number
	on events_row_number.clerk_name = previous_selling_order_number.clerk_name
	and events_row_number.asset_ticker = previous_selling_order_number.asset_ticker
	and events_row_number.ticker_row_num > coalesce(previous_selling_order_number.previous_selling_index, 0)
	and events_row_number.ticker_row_num <= previous_selling_order_number.selling_row_index
)
select 
	clerk_name,
	docid,
	asset_ticker,
	date,
	transaction_type,
	closing_price,
	est_shares,
	SUM(qty) OVER (
        PARTITION BY clerk_name, asset_ticker, selling_blocks
        ORDER BY ticker_row_num
    ) AS est_current_shares,
	SUM(-qty*closing_price) OVER (
        PARTITION BY clerk_name, asset_ticker, selling_blocks
        ORDER BY ticker_row_num
    ) AS est_current_profit
from trading_block_number
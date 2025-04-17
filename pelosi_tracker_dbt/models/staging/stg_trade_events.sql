{{
    config(
        materialized='view'
    )
}}

with trade_events as (
    select * 
    from {{ source('staging', 'houseclerk_trade_events') }}
)

select 
    cast(docid as int) as docid, 
    id, 
    owner,
    asset,
    (regexp_matches(asset, '\[(.*?)\]'))[1] as asset_type,
    (regexp_matches(asset, '\((.*?)\)'))[1] as asset_ticker,
    transaction_type,
    date, notification_date,
    amount,
    CAST(REPLACE((regexp_matches(amount, 
        '\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)'))[1], ',', '') AS INT) AS amount_range_low,
    CAST(REPLACE((regexp_matches(amount, 
        '\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)'))[2], ',', '') AS INT) AS amount_range_high,
    description,
    REGEXP_REPLACE((
        REGEXP_MATCHES(description, '([\d,]+)\s+shares'))[1],
        ',',
        '',
        'g'
    )::int AS number_of_shares,
    lower((REGEXP_MATCHES(description, '(\S+)\s*options'))[1])
         AS option_type,
    REGEXP_REPLACE((
        REGEXP_MATCHES(description, '([\d,]+)\s+(call|put)\s+options'))[1],
        ',',
        '',
        'g'
    )::int AS number_of_options,
    REGEXP_REPLACE((
		REGEXP_MATCHES(
			REGEXP_REPLACE(description, '.*strike\s*', ''),
    			'\$([0-9,.]+)'
  				))[1],
        	',',
        	'',
        	'g'
    	)::float AS strike_price,
    TO_DATE(
        (REGEXP_MATCHES(
            REGEXP_REPLACE(description, '.*expir\s*', ''),
            '([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})'
        ))[1],
        'MM/DD/YY'
    ) AS expiration_date,
    REGEXP_MATCHES(
            description,
            'options\s*purchased\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})(?:\s*&\s*[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})*',
            'g'
    ) AS purchase_date_option
from trade_events
where docid ~ '^[0-9\.]+$'
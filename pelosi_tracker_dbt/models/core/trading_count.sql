{{
    config(
        materialized='table'
    )
}}

select clerk_state, asset_type, count(*) as event_count
from {{ ref('doc_data') }}
group by clerk_state, asset_type
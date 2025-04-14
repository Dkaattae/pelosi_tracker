{{ config(materialized='table') }}

select 
    asset_code,
    asset_name
from {{ ref('Asset_Code_Description') }}
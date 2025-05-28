{{ config(materialized='table') }}

-- Only include valid non-null IDs
with source_data as (
    select 1 as id
    -- Replace with real data later
)

select *
from source_data
where id is not null
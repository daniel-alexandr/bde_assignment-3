-- {% snapshot snapshot_neighbourhood %}

-- {{
--         config(
--           target_schema='raw',
--           strategy='timestamp',
--           unique_key='host_id',
--           updated_at='scraped_date',
--         )
--     }}

-- select scraped_date,
-- host_id,
-- listing_neighbourhood,
-- host_neighbourhood
-- from {{ source('raw', 'listings') }}

-- {% endsnapshot %}
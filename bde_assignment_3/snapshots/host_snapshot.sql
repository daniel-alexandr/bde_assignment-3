{% snapshot snapshot_host %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='scraped_date',
        )
    }}

select scraped_date,
listing_id,
host_id,
host_name,
host_since,
host_is_superhost
from {{ source('raw', 'listings') }}

{% endsnapshot %}
{% snapshot snapshot_neighbourhood %}

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
listing_neighbourhood,
host_neighbourhood
from {{ source('raw', 'listings') }}

{% endsnapshot %}
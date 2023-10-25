{% snapshot snapshot_neighbourhood %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date'
        )
    }}

select distinct scraped_date,
listing_id,
listing_neighbourhood,
host_neighbourhood
from {{ source('raw', 'listings') }}


{% endsnapshot %}
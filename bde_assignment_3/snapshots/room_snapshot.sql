{% snapshot snapshot_room %}

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
room_type
from {{ source('raw', 'listings') }}

{% endsnapshot %}
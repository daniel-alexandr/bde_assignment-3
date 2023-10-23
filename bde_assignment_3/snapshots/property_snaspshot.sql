{% snapshot snapshot_property %}

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
property_type,
accommodates

 from {{ source('raw', 'listings') }}

{% endsnapshot %}
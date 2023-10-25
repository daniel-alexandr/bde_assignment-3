{% snapshot property_snapshot %}

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
property_type,
accommodates

 from {{ source('raw', 'listings') }}


{% endsnapshot %}
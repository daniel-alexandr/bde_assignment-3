{% snapshot snapshot_property %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='host_id',
          updated_at='scraped_date'
        )
    }}

select distinct scraped_date,
host_id,
property_type,
accommodates

 from {{ source('raw', 'listings') }}


{% endsnapshot %}